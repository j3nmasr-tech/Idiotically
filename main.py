#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ROMEOTPT v4.0 - PURE LIQUIDITY & MARKET STATE ENGINE
COMPLETE & CORRECTED IMPLEMENTATION
"""

import os
import time
import asyncio
import logging
import datetime
import json
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

# ---------------- CONFIG ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
DB_PATH = os.getenv("DB_PATH", "/app/data/romeopt_v4_0.db")

# Scanner settings
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 60))
TOP_N = int(os.getenv("TOP_N", 60))
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", 8))

# ROMEOTPT-specific settings
MIN_SWEEP_STRENGTH = float(os.getenv("MIN_SWEEP_STRENGTH", 0.7))
MIN_DISPLACEMENT_RATIO = float(os.getenv("MIN_DISPLACEMENT_RATIO", 0.6))
MAX_ENTRY_DISTANCE_PCT = float(os.getenv("MAX_ENTRY_DISTANCE_PCT", 0.5))

# Deduplication settings
SIGNAL_COOLDOWN_MINUTES = int(os.getenv("SIGNAL_COOLDOWN_MINUTES", 30))
SIGNAL_VALIDITY_HOURS = int(os.getenv("SIGNAL_VALIDITY_HOURS", 4))

# ---------------- ENUMS ----------------
class MarketState(Enum):
    ACCUMULATION = "ACCUMULATION"
    EXPANSION = "EXPANSION"
    DISTRIBUTION = "DISTRIBUTION"
    UNCLEAR = "UNCLEAR"

class SetupType(Enum):
    SWEEP_REVERSAL = "SWEEP_REVERSAL"
    PULLBACK_CONTINUATION = "PULLBACK_CONTINUATION"
    FAILED_BREAKOUT = "FAILED_BREAKOUT"

# ---------------- LOGGING ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("romeopt_v4_0")

# ---------------- DATA STRUCTURES ----------------
@dataclass
class HTFNarrative:
    """STEP 1: HTF NARRATIVE ENGINE"""
    market_type: str = ""
    range_high: float = 0.0
    range_low: float = 0.0
    premium_zone: Tuple[float, float] = (0.0, 0.0)
    discount_zone: Tuple[float, float] = (0.0, 0.0)
    external_liquidity_levels: List[float] = field(default_factory=list)
    is_at_extreme: bool = False
    mid_range: Tuple[float, float] = (0.0, 0.0)
    
    def to_dict(self):
        return {
            "market_type": self.market_type,
            "range_high": self.range_high,
            "range_low": self.range_low,
            "premium_zone": self.premium_zone,
            "discount_zone": self.discount_zone,
            "external_liquidity": self.external_liquidity_levels,
            "is_at_extreme": self.is_at_extreme,
            "mid_range": self.mid_range
        }

@dataclass
class LiquidityEvent:
    """STEP 2: LIQUIDITY ENGINE"""
    level: float = 0.0
    event_type: str = ""
    timeframe: str = ""
    is_taken: bool = False
    close_back_inside: bool = False
    displacement_away: bool = False
    strength: float = 0.0
    candle_index: int = -1
    
    def is_valid(self) -> bool:
        return self.is_taken and (self.close_back_inside or self.displacement_away) and self.strength >= MIN_SWEEP_STRENGTH

@dataclass
class MarketStateAnalysis:
    """STEP 3: MARKET STATE ENGINE"""
    state: MarketState = MarketState.UNCLEAR
    confidence: float = 0.0
    reasons: List[str] = field(default_factory=list)
    
    def to_dict(self):
        return {
            "state": self.state.value,
            "confidence": self.confidence,
            "reasons": self.reasons
        }

@dataclass
class DisplacementAnalysis:
    """STEP 5: DISPLACEMENT CONFIRMATION"""
    has_displacement: bool = False
    candle_body_ratio: float = 0.0
    closes_through_structure: bool = False
    leaves_inefficiency: bool = False
    displacement_direction: str = ""
    candle_data: Dict = field(default_factory=dict)
    
    def is_valid(self) -> bool:
        return (self.has_displacement and 
                self.candle_body_ratio >= MIN_DISPLACEMENT_RATIO and
                self.closes_through_structure and
                self.leaves_inefficiency)

@dataclass
class StructureShift:
    """STEP 5: MARKET STRUCTURE SHIFT"""
    has_shift: bool = False
    shift_type: str = ""
    broken_level: float = 0.0
    confirmation_price: float = 0.0
    timeframe: str = ""
    
    def is_valid(self) -> bool:
        return self.has_shift

@dataclass
class ROMEOTPTSignal:
    """Complete ROMEOTPT signal"""
    asset: str = ""
    direction: str = ""
    market_state: MarketState = MarketState.UNCLEAR
    setup_type: SetupType = None
    htf_narrative: Dict = field(default_factory=dict)
    liquidity_taken: List[Dict] = field(default_factory=list)
    entry_zone: Tuple[float, float] = (0.0, 0.0)
    entry_tf: str = ""
    take_profit_liquidity: float = 0.0
    reason: str = ""
    timestamp: str = ""
    current_price: float = 0.0
    signal_score: float = 0.0
    displacement_candle: Dict = field(default_factory=dict)
    
    def to_output_format(self) -> str:
        if not self.is_valid():
            return "NO TRADE — CONDITIONS NOT MET"
        
        return f"""ASSET: {self.asset}
DIRECTION: {self.direction}
MARKET STATE: {self.market_state.value}
SETUP TYPE: {self.setup_type.value if self.setup_type else 'NONE'}
HTF NARRATIVE: {json.dumps(self.htf_narrative, indent=2)}
LIQUIDITY TAKEN: {json.dumps(self.liquidity_taken, indent=2)}
ENTRY ZONE: {self.entry_zone[0]:.8f} - {self.entry_zone[1]:.8f}
ENTRY TF: {self.entry_tf}
TAKE PROFIT LIQUIDITY: {self.take_profit_liquidity:.8f}
REASON (CAUSE → EFFECT): {self.reason}"""
    
    def is_valid(self) -> bool:
        return all([
            self.asset,
            self.direction in ["LONG", "SHORT"],
            self.market_state != MarketState.UNCLEAR,
            self.setup_type is not None,
            self.take_profit_liquidity > 0,
            self.entry_zone[0] > 0,
            self.signal_score >= 0.7,
            self.entry_tf != ""
        ])

# ---------------- CORE ROMEOTPT ENGINE ----------------
class ROMEOTPTEngine:
    """Pure ROMEOTPT trading logic implementation"""
    
    def __init__(self):
        self.log = logging.getLogger("romeopt_engine")
    
    async def analyze_htf_narrative(self, exchange, symbol: str) -> HTFNarrative:
        """STEP 1: HTF NARRATIVE ENGINE (4H → 1H)"""
        narrative = HTFNarrative()
        
        try:
            ohlcv_4h = await self._fetch_ohlcv_with_timeout(exchange, symbol, "4h", 100)
            if not ohlcv_4h or len(ohlcv_4h) < 50:
                return narrative
            
            df_4h = pd.DataFrame(ohlcv_4h, columns=["timestamp", "open", "high", "low", "close", "volume"])
            
            recent_high = df_4h['high'].iloc[-20:].max()
            recent_low = df_4h['low'].iloc[-20:].min()
            range_height = (recent_high - recent_low) / recent_low * 100
            
            current_price = df_4h['close'].iloc[-1]
            
            if range_height < 8.0:
                narrative.market_type = "RANGE"
                narrative.range_high = recent_high
                narrative.range_low = recent_low
                
                range_mid = (recent_high + recent_low) / 2
                range_quarter = (recent_high - recent_low) / 4
                
                narrative.premium_zone = (recent_high - range_quarter, recent_high)
                narrative.discount_zone = (recent_low, recent_low + range_quarter)
                narrative.mid_range = (range_mid - range_quarter, range_mid + range_quarter)
                
                narrative.is_at_extreme = (
                    current_price >= narrative.premium_zone[0] or 
                    current_price <= narrative.discount_zone[1]
                )
            else:
                narrative.market_type = "TREND"
                swing_highs = self._find_swing_highs(df_4h, 5)
                swing_lows = self._find_swing_lows(df_4h, 5)
                
                if swing_highs:
                    narrative.range_high = max(swing_highs[-3:]) if len(swing_highs) >= 3 else swing_highs[-1]
                if swing_lows:
                    narrative.range_low = min(swing_lows[-3:]) if len(swing_lows) >= 3 else swing_lows[-1]
                
                narrative.premium_zone = (narrative.range_high * 0.98, narrative.range_high)
                narrative.discount_zone = (narrative.range_low, narrative.range_low * 1.02)
                narrative.mid_range = (narrative.range_low * 1.02, narrative.range_high * 0.98)
                narrative.is_at_extreme = False
            
            narrative.external_liquidity_levels = self._find_external_liquidity(df_4h)
            
        except Exception as e:
            self.log.debug(f"HTF narrative error for {symbol}: {e}")
        
        return narrative
    
    async def detect_liquidity_events(self, exchange, symbol: str, narrative: HTFNarrative) -> List[LiquidityEvent]:
        """STEP 2: LIQUIDITY ENGINE - Detect REAL stop-taking"""
        events = []
        
        try:
            timeframes = ["4h", "1h", "30m", "15m"]
            
            for tf in timeframes:
                ohlcv = await self._fetch_ohlcv_with_timeout(exchange, symbol, tf, 100)
                if not ohlcv or len(ohlcv) < 30:
                    continue
                
                df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
                
                # Check last 10 candles for liquidity events
                for i in range(-10, 0):
                    if abs(i) >= len(df):
                        continue
                    
                    candle = df.iloc[i]
                    prev_candle = df.iloc[i-1] if i > -len(df) else None
                    next_candle = df.iloc[i+1] if i < -1 else None
                    
                    # Check for swing point liquidity
                    is_swing_high = False
                    is_swing_low = False
                    
                    if i < -2 and i > -len(df) + 2:
                        is_swing_high = (
                            candle['high'] > df['high'].iloc[i-2] and
                            candle['high'] > df['high'].iloc[i-1] and
                            candle['high'] > df['high'].iloc[i+1] and
                            candle['high'] > df['high'].iloc[i+2]
                        )
                        
                        is_swing_low = (
                            candle['low'] < df['low'].iloc[i-2] and
                            candle['low'] < df['low'].iloc[i-1] and
                            candle['low'] < df['low'].iloc[i+1] and
                            candle['low'] < df['low'].iloc[i+2]
                        )
                    
                    # Check equal highs (within 0.1%)
                    if not is_swing_high:
                        high_window = df['high'].iloc[max(i-3, 0):min(i+4, len(df))]
                        similar_highs = high_window[
                            abs(high_window - candle['high']) / candle['high'] < 0.001
                        ]
                        is_equal_high = len(similar_highs) >= 2
                    else:
                        is_equal_high = False
                    
                    # Check equal lows (within 0.1%)
                    if not is_swing_low:
                        low_window = df['low'].iloc[max(i-3, 0):min(i+4, len(df))]
                        similar_lows = low_window[
                            abs(low_window - candle['low']) / candle['low'] < 0.001
                        ]
                        is_equal_low = len(similar_lows) >= 2
                    else:
                        is_equal_low = False
                    
                    # Current candle for validation
                    current_candle = df.iloc[-1]
                    
                    # Process swing highs
                    if is_swing_high:
                        event = LiquidityEvent(
                            level=candle['high'],
                            event_type="SWING_HIGH",
                            timeframe=tf,
                            is_taken=current_candle['high'] > candle['high'],
                            close_back_inside=(
                                current_candle['close'] < candle['high'] and 
                                next_candle is not None and 
                                next_candle['close'] < candle['high']
                            ) if next_candle else False,
                            displacement_away=(
                                current_candle['close'] < prev_candle['low'] if prev_candle else False
                            ),
                            candle_index=i
                        )
                        if event.is_taken:
                            event.strength = self._calculate_sweep_strength(df, i, "HIGH")
                            if event.strength >= MIN_SWEEP_STRENGTH:
                                events.append(event)
                    
                    # Process swing lows
                    if is_swing_low:
                        event = LiquidityEvent(
                            level=candle['low'],
                            event_type="SWING_LOW",
                            timeframe=tf,
                            is_taken=current_candle['low'] < candle['low'],
                            close_back_inside=(
                                current_candle['close'] > candle['low'] and
                                next_candle is not None and
                                next_candle['close'] > candle['low']
                            ) if next_candle else False,
                            displacement_away=(
                                current_candle['close'] > prev_candle['high'] if prev_candle else False
                            ),
                            candle_index=i
                        )
                        if event.is_taken:
                            event.strength = self._calculate_sweep_strength(df, i, "LOW")
                            if event.strength >= MIN_SWEEP_STRENGTH:
                                events.append(event)
                    
                    # Process equal highs
                    if is_equal_high and not is_swing_high:
                        event = LiquidityEvent(
                            level=candle['high'],
                            event_type="EQUAL_HIGH",
                            timeframe=tf,
                            is_taken=current_candle['high'] > candle['high'],
                            close_back_inside=current_candle['close'] < candle['high'],
                            displacement_away=(
                                current_candle['close'] < prev_candle['low'] if prev_candle else False
                            ),
                            candle_index=i
                        )
                        if event.is_taken:
                            event.strength = 0.7  # Base strength for equal highs
                            events.append(event)
                    
                    # Process equal lows
                    if is_equal_low and not is_swing_low:
                        event = LiquidityEvent(
                            level=candle['low'],
                            event_type="EQUAL_LOW",
                            timeframe=tf,
                            is_taken=current_candle['low'] < candle['low'],
                            close_back_inside=current_candle['close'] > candle['low'],
                            displacement_away=(
                                current_candle['close'] > prev_candle['high'] if prev_candle else False
                            ),
                            candle_index=i
                        )
                        if event.is_taken:
                            event.strength = 0.7  # Base strength for equal lows
                            events.append(event)
                
                # Check range sweeps
                current_candle = df.iloc[-1]
                prev_candle = df.iloc[-2] if len(df) >= 2 else None
                
                if narrative.range_high > 0 and current_candle['high'] > narrative.range_high:
                    event = LiquidityEvent(
                        level=narrative.range_high,
                        event_type="RANGE_HIGH_SWEEP",
                        timeframe=tf,
                        is_taken=True,
                        close_back_inside=current_candle['close'] < narrative.range_high,
                        displacement_away=(
                            current_candle['close'] < prev_candle['low'] if prev_candle else False
                        ),
                        strength=0.8
                    )
                    events.append(event)
                
                if narrative.range_low > 0 and current_candle['low'] < narrative.range_low:
                    event = LiquidityEvent(
                        level=narrative.range_low,
                        event_type="RANGE_LOW_SWEEP",
                        timeframe=tf,
                        is_taken=True,
                        close_back_inside=current_candle['close'] > narrative.range_low,
                        displacement_away=(
                            current_candle['close'] > prev_candle['high'] if prev_candle else False
                        ),
                        strength=0.8
                    )
                    events.append(event)
        
        except Exception as e:
            self.log.debug(f"Liquidity detection error for {symbol}: {e}")
        
        # Filter for strongest and most recent events
        if events:
            # Sort by strength then recency
            events.sort(key=lambda x: (x.strength, x.candle_index), reverse=True)
            # Take top 2 strongest events
            events = events[:2]
        
        return events
    
    async def determine_market_state(self, exchange, symbol: str, narrative: HTFNarrative) -> MarketStateAnalysis:
        """STEP 3: MARKET STATE ENGINE"""
        analysis = MarketStateAnalysis()
        
        try:
            ohlcv_1h = await self._fetch_ohlcv_with_timeout(exchange, symbol, "1h", 100)
            if not ohlcv_1h or len(ohlcv_1h) < 30:
                analysis.state = MarketState.UNCLEAR
                return analysis
            
            df_1h = pd.DataFrame(ohlcv_1h, columns=["timestamp", "open", "high", "low", "close", "volume"])
            
            # Get recent price action
            recent_df = df_1h.iloc[-20:]
            recent_high = recent_df['high'].max()
            recent_low = recent_df['low'].min()
            current_price = recent_df['close'].iloc[-1]
            range_height_pct = (recent_high - recent_low) / recent_low * 100
            
            # === ACCUMULATION DETECTION ===
            # Look for multiple sweeps with rejections
            sweep_count = 0
            rejection_count = 0
            
            for i in range(-15, -1):
                if abs(i) >= len(df_1h) - 1:
                    continue
                
                candle = df_1h.iloc[i]
                next_candle = df_1h.iloc[i+1]
                
                # Check for high sweep and rejection
                if candle['high'] > df_1h['high'].iloc[max(i-5, 0):i].max():
                    if next_candle['close'] < candle['close']:
                        sweep_count += 1
                        if next_candle['close'] < next_candle['open']:
                            rejection_count += 1
                
                # Check for low sweep and rejection
                elif candle['low'] < df_1h['low'].iloc[max(i-5, 0):i].min():
                    if next_candle['close'] > candle['close']:
                        sweep_count += 1
                        if next_candle['close'] > next_candle['open']:
                            rejection_count += 1
            
            accumulation_score = (sweep_count / 15) * 0.5 + (rejection_count / max(sweep_count, 1)) * 0.5
            
            if accumulation_score > 0.4 and range_height_pct < 6:
                analysis.state = MarketState.ACCUMULATION
                analysis.confidence = min(0.9, accumulation_score)
                analysis.reasons.append(f"{sweep_count} sweeps with {rejection_count} rejections")
                analysis.reasons.append(f"Tight range: {range_height_pct:.1f}%")
                return analysis
            
            # === EXPANSION DETECTION ===
            # Check for strong directional movement
            directional_strength = 0
            strong_bars = 0
            
            for i in range(-8, 0):
                if abs(i) >= len(df_1h):
                    continue
                
                candle = df_1h.iloc[i]
                body_size = abs(candle['close'] - candle['open'])
                total_range = candle['high'] - candle['low']
                
                if body_size / total_range > 0.7:  # Strong real body
                    strong_bars += 1
                    if candle['close'] > candle['open']:
                        directional_strength += 1
                    else:
                        directional_strength -= 1
            
            expansion_score = (strong_bars / 8) * 0.5 + (abs(directional_strength) / 8) * 0.5
            
            if expansion_score > 0.5 and abs(directional_strength) >= 4:
                analysis.state = MarketState.EXPANSION
                analysis.confidence = min(0.9, expansion_score)
                direction = "BULLISH" if directional_strength > 0 else "BEARISH"
                analysis.reasons.append(f"Strong {direction} expansion")
                analysis.reasons.append(f"{strong_bars} strong bars, net direction: {directional_strength}")
                return analysis
            
            # === DISTRIBUTION DETECTION ===
            # Need prior trend + weakening momentum
            if len(df_1h) >= 50:
                # Check 4H trend via multiple 1H candles
                old_close = df_1h['close'].iloc[-50]
                recent_close = df_1h['close'].iloc[-10]
                trend_pct = (recent_close - old_close) / old_close * 100
                
                if abs(trend_pct) > 5:  # Significant prior trend
                    # Check recent momentum
                    recent_momentum = self._calculate_momentum(df_1h.iloc[-10:])
                    early_momentum = self._calculate_momentum(df_1h.iloc[-20:-10])
                    
                    momentum_ratio = abs(recent_momentum) / max(abs(early_momentum), 0.001)
                    
                    if momentum_ratio < 0.5:  # Momentum weakened by >50%
                        analysis.state = MarketState.DISTRIBUTION
                        analysis.confidence = 0.7
                        trend_dir = "UP" if trend_pct > 0 else "DOWN"
                        analysis.reasons.append(f"Prior {trend_dir} trend ({trend_pct:.1f}%)")
                        analysis.reasons.append(f"Momentum weakened: {momentum_ratio:.2f}")
                        return analysis
            
            analysis.state = MarketState.UNCLEAR
            analysis.confidence = 0.3
            
        except Exception as e:
            self.log.debug(f"Market state error for {symbol}: {e}")
            analysis.state = MarketState.UNCLEAR
        
        return analysis
    
    async def analyze_displacement(self, exchange, symbol: str, direction: str, liquidity_tf: str) -> DisplacementAnalysis:
        """STEP 5.2: DISPLACEMENT ANALYSIS"""
        analysis = DisplacementAnalysis()
        
        try:
            # Determine appropriate TF for displacement based on liquidity TF
            tf_map = {
                "4h": "1h",
                "1h": "30m",
                "30m": "15m",
                "15m": "5m",
                "5m": "3m"
            }
            
            displacement_tf = tf_map.get(liquidity_tf, "15m")
            
            ohlcv = await self._fetch_ohlcv_with_timeout(exchange, symbol, displacement_tf, 20)
            if not ohlcv or len(ohlcv) < 5:
                return analysis
            
            df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
            
            # Look for displacement candle in last 3-5 candles
            for i in range(-5, 0):
                if abs(i) >= len(df):
                    continue
                
                candle = df.iloc[i]
                prev_candle = df.iloc[i-1] if i > -len(df) else None
                next_candle = df.iloc[i+1] if i < -1 else None
                
                body_size = abs(candle['close'] - candle['open'])
                total_range = candle['high'] - candle['low']
                
                if total_range == 0:
                    continue
                
                body_ratio = body_size / total_range
                
                if body_ratio >= MIN_DISPLACEMENT_RATIO:
                    # Check if it's directional
                    is_bullish = candle['close'] > candle['open']
                    is_bearish = candle['close'] < candle['open']
                    
                    if (direction == "LONG" and is_bullish) or (direction == "SHORT" and is_bearish):
                        analysis.has_displacement = True
                        analysis.candle_body_ratio = body_ratio
                        analysis.displacement_direction = "BULLISH" if is_bullish else "BEARISH"
                        analysis.candle_data = {
                            'open': float(candle['open']),
                            'high': float(candle['high']),
                            'low': float(candle['low']),
                            'close': float(candle['close']),
                            'timestamp': candle['timestamp']
                        }
                        
                        # Check if closes through structure
                        if prev_candle is not None:
                            if direction == "LONG":
                                analysis.closes_through_structure = candle['close'] > prev_candle['high']
                                analysis.leaves_inefficiency = candle['low'] > prev_candle['high']
                            else:
                                analysis.closes_through_structure = candle['close'] < prev_candle['low']
                                analysis.leaves_inefficiency = candle['high'] < prev_candle['low']
                        
                        # Found valid displacement candle
                        break
        
        except Exception as e:
            self.log.debug(f"Displacement analysis error for {symbol}: {e}")
        
        return analysis
    
    async def analyze_structure_shift(self, exchange, symbol: str, direction: str, entry_tf: str) -> StructureShift:
        """STEP 5.3: MARKET STRUCTURE SHIFT"""
        shift = StructureShift()
        
        try:
            ohlcv = await self._fetch_ohlcv_with_timeout(exchange, symbol, entry_tf, 50)
            if not ohlcv or len(ohlcv) < 20:
                return shift
            
            df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
            
            # Find recent swing points
            swing_highs = []
            swing_lows = []
            
            for i in range(2, len(df) - 2):
                if (df['high'].iloc[i] > df['high'].iloc[i-1] and 
                    df['high'].iloc[i] > df['high'].iloc[i-2] and
                    df['high'].iloc[i] > df['high'].iloc[i+1] and
                    df['high'].iloc[i] > df['high'].iloc[i+2]):
                    swing_highs.append((i, df['high'].iloc[i]))
                
                if (df['low'].iloc[i] < df['low'].iloc[i-1] and 
                    df['low'].iloc[i] < df['low'].iloc[i-2] and
                    df['low'].iloc[i] < df['low'].iloc[i+1] and
                    df['low'].iloc[i] < df['low'].iloc[i+2]):
                    swing_lows.append((i, df['low'].iloc[i]))
            
            current_price = df['close'].iloc[-1]
            
            if direction == "LONG":
                # Need to break a lower high (downtrend structure)
                if len(swing_highs) >= 2:
                    recent_high = swing_highs[-1][1]
                    prev_high = swing_highs[-2][1] if len(swing_highs) >= 2 else 0
                    
                    if prev_high > recent_high:  # Lower high pattern
                        shift.has_shift = current_price > recent_high
                        shift.shift_type = "BULLISH_BREAK"
                        shift.broken_level = recent_high
                        shift.confirmation_price = current_price
                        shift.timeframe = entry_tf
            
            else:  # SHORT
                # Need to break a higher low (uptrend structure)
                if len(swing_lows) >= 2:
                    recent_low = swing_lows[-1][1]
                    prev_low = swing_lows[-2][1] if len(swing_lows) >= 2 else float('inf')
                    
                    if prev_low < recent_low:  # Higher low pattern
                        shift.has_shift = current_price < recent_low
                        shift.shift_type = "BEARISH_BREAK"
                        shift.broken_level = recent_low
                        shift.confirmation_price = current_price
                        shift.timeframe = entry_tf
        
        except Exception as e:
            self.log.debug(f"Structure shift error for {symbol}: {e}")
        
        return shift
    
    async def determine_entry_tf(self, liquidity_tf: str) -> str:
        """Determine entry TF based on liquidity TF"""
        tf_mapping = {
            "4h": "15m",
            "1h": "15m",
            "30m": "5m",
            "15m": "5m",
            "5m": "1m"
        }
        return tf_mapping.get(liquidity_tf, "15m")
    
    async def determine_entry_zone(self, direction: str, displacement_candle: Dict, current_price: float) -> Tuple[float, float]:
        """Determine entry zone based on displacement candle"""
        if not displacement_candle:
            # Fallback to tight zone around current price
            if direction == "LONG":
                return (
                    current_price * (1 - MAX_ENTRY_DISTANCE_PCT/200),
                    current_price * (1 + MAX_ENTRY_DISTANCE_PCT/200)
                )
            else:
                return (
                    current_price * (1 - MAX_ENTRY_DISTANCE_PCT/200),
                    current_price * (1 + MAX_ENTRY_DISTANCE_PCT/200)
                )
        
        candle_close = displacement_candle.get('close', current_price)
        
        if direction == "LONG":
            # Entry just above displacement candle close
            return (
                candle_close * 0.999,  # 0.1% below
                candle_close * 1.002   # 0.2% above
            )
        else:
            # Entry just below displacement candle close
            return (
                candle_close * 0.998,  # 0.2% below
                candle_close * 1.001   # 0.1% above
            )
    
    async def find_take_profit_liquidity(self, exchange, symbol: str, direction: str, 
                                       narrative: HTFNarrative, entry_tf: str) -> float:
        """STEP 6: TAKE PROFIT ENGINE - Find untouched external liquidity"""
        
        # TP must be on higher TF than entry
        higher_tf_map = {
            "1m": ["5m", "15m", "30m", "1h"],
            "5m": ["15m", "30m", "1h", "4h"],
            "15m": ["30m", "1h", "4h"],
            "30m": ["1h", "4h"],
            "1h": ["4h"]
        }
        
        target_tfs = higher_tf_map.get(entry_tf, ["1h", "4h"])
        
        try:
            for tf in target_tfs:
                ohlcv = await self._fetch_ohlcv_with_timeout(exchange, symbol, tf, 100)
                if not ohlcv or len(ohlcv) < 30:
                    continue
                
                df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
                current_price = df['close'].iloc[-1]
                
                if direction == "LONG":
                    # Find untouched swing highs
                    swing_highs = self._find_swing_highs(df, 5)
                    
                    for high in reversed(swing_highs[-10:]):  # Check most recent first
                        if high > current_price * 1.005:  # Must be above current price
                            # Check if it's been taken recently (last 5 candles)
                            recent_max = df['high'].iloc[-5:].max()
                            if high > recent_max:  # Untouched
                                return high
                
                else:  # SHORT
                    # Find untouched swing lows
                    swing_lows = self._find_swing_lows(df, 5)
                    
                    for low in reversed(swing_lows[-10:]):
                        if low < current_price * 0.995:  # Must be below current price
                            recent_min = df['low'].iloc[-5:].min()
                            if low < recent_min:  # Untouched
                                return low
        
        except Exception as e:
            self.log.debug(f"TP liquidity error for {symbol}: {e}")
        
        # Fallback to HTF narrative levels
        current_price = await self._get_current_price(exchange, symbol)
        
        if direction == "LONG" and narrative.external_liquidity_levels:
            for level in sorted(narrative.external_liquidity_levels):
                if level > current_price * 1.01:
                    return level
        
        elif direction == "SHORT" and narrative.external_liquidity_levels:
            for level in sorted(narrative.external_liquidity_levels, reverse=True):
                if level < current_price * 0.99:
                    return level
        
        return 0.0
    
    async def check_entry_tf_invalidation(self, exchange, symbol: str, entry_tf: str, 
                                        direction: str, entry_zone: Tuple[float, float]) -> bool:
        """Check if entry TF has invalidated the setup"""
        try:
            ohlcv = await self._fetch_ohlcv_with_timeout(exchange, symbol, entry_tf, 3)
            if not ohlcv or len(ohlcv) < 2:
                return True  # Invalid if no data
            
            df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
            
            latest_candle = df.iloc[-1]
            entry_low, entry_high = entry_zone
            
            if direction == "LONG":
                # Invalid if price closes below entry zone
                if latest_candle['close'] < entry_low:
                    return True
                # Invalid if strong rejection candle forms
                if latest_candle['close'] < latest_candle['open'] and abs(latest_candle['close'] - latest_candle['open']) / latest_candle['open'] > 0.003:
                    return True
            
            else:  # SHORT
                # Invalid if price closes above entry zone
                if latest_candle['close'] > entry_high:
                    return True
                # Invalid if strong rejection candle forms
                if latest_candle['close'] > latest_candle['open'] and abs(latest_candle['close'] - latest_candle['open']) / latest_candle['open'] > 0.003:
                    return True
            
            return False
        
        except Exception as e:
            self.log.debug(f"Entry TF invalidation check error: {e}")
            return True  # Invalid on error
    
    async def determine_expansion_direction(self, exchange, symbol: str) -> str:
        """Determine expansion direction from structure analysis"""
        try:
            ohlcv_1h = await self._fetch_ohlcv_with_timeout(exchange, symbol, "1h", 30)
            if not ohlcv_1h:
                return "LONG"
            
            df_1h = pd.DataFrame(ohlcv_1h, columns=["timestamp", "open", "high", "low", "close", "volume"])
            
            # Check for recent structure break
            swing_highs = self._find_swing_highs(df_1h, 3)
            swing_lows = self._find_swing_lows(df_1h, 3)
            
            current_price = df_1h['close'].iloc[-1]
            
            if swing_highs and current_price > swing_highs[-1]:
                return "LONG"
            
            if swing_lows and current_price < swing_lows[-1]:
                return "SHORT"
            
            # Check momentum
            momentum_5 = self._calculate_momentum(df_1h.iloc[-5:])
            momentum_10 = self._calculate_momentum(df_1h.iloc[-10:])
            
            if momentum_5 > 0 and momentum_10 > 0:
                return "LONG"
            elif momentum_5 < 0 and momentum_10 < 0:
                return "SHORT"
            
            # Default based on recent close
            return "LONG" if df_1h['close'].iloc[-1] > df_1h['close'].iloc[-5] else "SHORT"
            
        except Exception as e:
            self.log.debug(f"Expansion direction error: {e}")
            return "LONG"
    
    async def generate_signal(self, exchange, symbol: str) -> Optional[ROMEOTPTSignal]:
        """Complete ROMEOTPT signal generation"""
        
        signal = ROMEOTPTSignal(asset=symbol)
        signal.timestamp = datetime.datetime.utcnow().isoformat()
        
        try:
            # === STEP 1: HTF NARRATIVE ===
            narrative = await self.analyze_htf_narrative(exchange, symbol)
            signal.htf_narrative = narrative.to_dict()
            
            # Rule: If price is mid-range → STOP
            if narrative.market_type == "RANGE":
                current_price = await self._get_current_price(exchange, symbol)
                if current_price > 0 and (narrative.mid_range[0] <= current_price <= narrative.mid_range[1]):
                    self.log.debug(f"{symbol}: Price in mid-range, no trade")
                    return None
            
            # === STEP 2: LIQUIDITY DETECTION ===
            liquidity_events = await self.detect_liquidity_events(exchange, symbol, narrative)
            
            # Rule: If no external liquidity taken → STOP
            if not liquidity_events:
                self.log.debug(f"{symbol}: No valid liquidity events")
                return None
            
            signal.liquidity_taken = [{
                "level": e.level,
                "type": e.event_type,
                "timeframe": e.timeframe,
                "strength": e.strength
            } for e in liquidity_events]
            
            # Take strongest liquidity event
            strongest_event = max(liquidity_events, key=lambda x: x.strength)
            
            # === STEP 3: MARKET STATE ===
            market_state = await self.determine_market_state(exchange, symbol, narrative)
            signal.market_state = market_state.state
            
            # Rule: If state unclear → STOP
            if market_state.state == MarketState.UNCLEAR or market_state.confidence < 0.6:
                self.log.debug(f"{symbol}: Market state unclear (confidence: {market_state.confidence:.2f})")
                return None
            
            # === STEP 4: SETUP SELECTION ===
            if market_state.state == MarketState.ACCUMULATION:
                setup_type = SetupType.SWEEP_REVERSAL
                # Direction based on liquidity event type
                if strongest_event.event_type in ["SWING_LOW", "EQUAL_LOW", "RANGE_LOW_SWEEP"]:
                    direction = "LONG"
                else:
                    direction = "SHORT"
                    
            elif market_state.state == MarketState.EXPANSION:
                setup_type = SetupType.PULLBACK_CONTINUATION
                direction = await self.determine_expansion_direction(exchange, symbol)
                
            elif market_state.state == MarketState.DISTRIBUTION:
                setup_type = SetupType.FAILED_BREAKOUT
                direction = "SHORT"  # Typically short in distribution
                
            else:
                return None
            
            signal.setup_type = setup_type
            signal.direction = direction
            
            # === STEP 5: ENTRY LOGIC ===
            # 5.1 Liquidity Taken (already confirmed)
            
            # 5.2 Displacement
            displacement = await self.analyze_displacement(
                exchange, symbol, direction, strongest_event.timeframe
            )
            
            if not displacement.is_valid():
                self.log.debug(f"{symbol}: No valid displacement")
                return None
            
            signal.displacement_candle = displacement.candle_data
            
            # 5.3 Market Structure Shift
            entry_tf = await self.determine_entry_tf(strongest_event.timeframe)
            structure_shift = await self.analyze_structure_shift(
                exchange, symbol, direction, entry_tf
            )
            
            if not structure_shift.is_valid():
                self.log.debug(f"{symbol}: No structure shift")
                return None
            
            # 5.4 Entry Location
            current_price = await self._get_current_price(exchange, symbol)
            signal.current_price = current_price
            
            if direction == "LONG":
                if not (narrative.discount_zone[0] <= current_price <= narrative.discount_zone[1]):
                    self.log.debug(f"{symbol}: Long not in discount zone")
                    return None
            else:
                if not (narrative.premium_zone[0] <= current_price <= narrative.premium_zone[1]):
                    self.log.debug(f"{symbol}: Short not in premium zone")
                    return None
            
            # Determine entry zone based on displacement candle
            entry_zone = await self.determine_entry_zone(
                direction, displacement.candle_data, current_price
            )
            
            signal.entry_zone = entry_zone
            signal.entry_tf = entry_tf
            
            # Check entry TF invalidation
            if await self.check_entry_tf_invalidation(exchange, symbol, entry_tf, direction, entry_zone):
                self.log.debug(f"{symbol}: Entry TF invalidated")
                return None
            
            # === STEP 6: TAKE PROFIT ===
            tp_liquidity = await self.find_take_profit_liquidity(
                exchange, symbol, direction, narrative, entry_tf
            )
            
            if tp_liquidity == 0:
                self.log.debug(f"{symbol}: No clean TP liquidity found")
                return None
            
            signal.take_profit_liquidity = tp_liquidity
            
            # === SIGNAL REASON ===
            signal.reason = (
                f"{strongest_event.event_type} at {strongest_event.level:.4f} on {strongest_event.timeframe} → "
                f"{direction} {setup_type.value} in {market_state.state.value}"
            )
            
            # === SCORE CALCULATION ===
            signal.signal_score = self._calculate_signal_score(
                strongest_event.strength,
                market_state.confidence,
                displacement.candle_body_ratio,
                1.0 if structure_shift.has_shift else 0.0,
                min(1.0, abs(tp_liquidity - current_price) / current_price * 10)  # TP distance factor
            )
            
            if signal.signal_score < 0.7:
                self.log.debug(f"{symbol}: Signal score too low ({signal.signal_score:.2f})")
                return None
            
            return signal
            
        except Exception as e:
            self.log.error(f"Signal generation error for {symbol}: {e}")
            return None
    
    # ============ HELPER METHODS ============
    
    async def _fetch_ohlcv_with_timeout(self, exchange, symbol: str, timeframe: str, limit: int):
        try:
            return await asyncio.wait_for(
                exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit),
                timeout=5.0
            )
        except Exception as e:
            self.log.debug(f"Failed to fetch {symbol} {timeframe}: {e}")
            return None
    
    async def _get_current_price(self, exchange, symbol: str) -> float:
        try:
            ticker = await exchange.fetch_ticker(symbol)
            return ticker.get('last', 0)
        except:
            return 0
    
    def _find_swing_highs(self, df: pd.DataFrame, lookback: int = 3) -> List[float]:
        highs = []
        for i in range(lookback, len(df) - lookback):
            if df['high'].iloc[i] == df['high'].iloc[i-lookback:i+lookback+1].max():
                highs.append(df['high'].iloc[i])
        return highs
    
    def _find_swing_lows(self, df: pd.DataFrame, lookback: int = 3) -> List[float]:
        lows = []
        for i in range(lookback, len(df) - lookback):
            if df['low'].iloc[i] == df['low'].iloc[i-lookback:i+lookback+1].min():
                lows.append(df['low'].iloc[i])
        return lows
    
    def _find_external_liquidity(self, df: pd.DataFrame) -> List[float]:
        levels = []
        swing_highs = self._find_swing_highs(df, 5)
        swing_lows = self._find_swing_lows(df, 5)
        
        for high in swing_highs[-10:]:
            levels.append(high)
        for low in swing_lows[-10:]:
            levels.append(low)
        
        return sorted(set(levels))
    
    def _calculate_sweep_strength(self, df: pd.DataFrame, sweep_idx: int, sweep_type: str) -> float:
        if sweep_idx >= len(df) - 1:
            return 0.5
        
        sweep_candle = df.iloc[sweep_idx]
        next_candle = df.iloc[sweep_idx + 1]
        
        strength = 0.5
        
        if sweep_type == "HIGH":
            wick_size = sweep_candle['high'] - max(sweep_candle['open'], sweep_candle['close'])
            body_size = abs(sweep_candle['close'] - sweep_candle['open'])
            if body_size > 0:
                wick_ratio = wick_size / body_size
                if wick_ratio > 1.5:
                    strength += 0.2
                elif wick_ratio > 2.0:
                    strength += 0.3
            
            if next_candle['close'] < sweep_candle['close']:
                strength += 0.3
        
        else:
            wick_size = min(sweep_candle['open'], sweep_candle['close']) - sweep_candle['low']
            body_size = abs(sweep_candle['close'] - sweep_candle['open'])
            if body_size > 0:
                wick_ratio = wick_size / body_size
                if wick_ratio > 1.5:
                    strength += 0.2
                elif wick_ratio > 2.0:
                    strength += 0.3
            
            if next_candle['close'] > sweep_candle['close']:
                strength += 0.3
        
        return min(strength, 1.0)
    
    def _calculate_momentum(self, df: pd.DataFrame) -> float:
        if len(df) < 2:
            return 0.0
        early_close = df['close'].iloc[0]
        late_close = df['close'].iloc[-1]
        return (late_close - early_close) / early_close
    
    def _calculate_signal_score(self, sweep_strength: float, state_confidence: float,
                              displacement_ratio: float, structure_score: float, tp_factor: float) -> float:
        weights = [0.25, 0.25, 0.2, 0.2, 0.1]
        scores = [sweep_strength, state_confidence, displacement_ratio, structure_score, tp_factor]
        return sum(w * s for w, s in zip(weights, scores))

# ---------------- SIGNAL TRACKER ----------------
class ROMEOTPTSignalTracker:
    
    def __init__(self):
        self.active_signals = {}
        self.signal_history = []
    
    def should_alert(self, signal: ROMEOTPTSignal) -> Tuple[bool, str]:
        symbol = signal.asset
        
        if symbol not in self.active_signals:
            return True, "New signal"
        
        old_data = self.active_signals[symbol]
        old_signal = old_data.get('signal')
        
        if not old_signal:
            return True, "Old signal corrupted"
        
        old_time = datetime.datetime.fromisoformat(old_signal.timestamp)
        new_time = datetime.datetime.fromisoformat(signal.timestamp)
        age_hours = (new_time - old_time).total_seconds() / 3600
        
        if age_hours > SIGNAL_VALIDITY_HOURS:
            self.remove_signal(symbol)
            return True, "Old signal expired"
        
        if old_signal.setup_type != signal.setup_type:
            return True, "Setup type changed"
        
        if old_signal.direction != signal.direction:
            return True, "Direction changed"
        
        tp_diff = abs(old_signal.take_profit_liquidity - signal.take_profit_liquidity)
        tp_diff_pct = tp_diff / old_signal.take_profit_liquidity * 100 if old_signal.take_profit_liquidity > 0 else 0
        
        if tp_diff_pct > 2.0:
            return True, f"TP moved {tp_diff_pct:.1f}%"
        
        if signal.signal_score - old_signal.signal_score > 0.1:
            return True, f"Score improved {old_signal.signal_score:.2f}→{signal.signal_score:.2f}"
        
        last_alerted = old_data.get('last_alerted')
        if last_alerted:
            time_since_alert = (new_time - last_alerted).total_seconds() / 60
            if time_since_alert < SIGNAL_COOLDOWN_MINUTES:
                return False, f"In cooldown ({int(SIGNAL_COOLDOWN_MINUTES - time_since_alert)}min left)"
        
        return False, "Similar signal active"
    
    def update_signal(self, signal: ROMEOTPTSignal, alerted: bool = False):
        symbol = signal.asset
        
        signal_data = {
            'signal': signal,
            'first_seen': datetime.datetime.fromisoformat(signal.timestamp),
            'alert_count': 0,
            'status': 'active'
        }
        
        if alerted:
            signal_data['last_alerted'] = datetime.datetime.utcnow()
            signal_data['alert_count'] = 1
        
        self.active_signals[symbol] = signal_data
    
    def remove_signal(self, symbol: str):
        if symbol in self.active_signals:
            signal_data = self.active_signals.pop(symbol)
            signal_data['status'] = 'expired'
            signal_data['expired_at'] = datetime.datetime.utcnow()
            self.signal_history.append(signal_data)
    
    def cleanup_old_signals(self):
        now = datetime.datetime.utcnow()
        expired = []
        
        for symbol, data in self.active_signals.items():
            age_hours = (now - data['first_seen']).total_seconds() / 3600
            if age_hours > SIGNAL_VALIDITY_HOURS:
                expired.append(symbol)
        
        for symbol in expired:
            self.remove_signal(symbol)
        
        if expired:
            log.info(f"Cleaned up {len(expired)} expired signals")

# ---------------- TELEGRAM ----------------
async def send_romeopt_alert(signal: ROMEOTPTSignal):
    try:
        output = signal.to_output_format()
        
        emoji = "🟢" if signal.direction == "LONG" else "🔴"
        state_emoji = {
            MarketState.ACCUMULATION: "🟡",
            MarketState.EXPANSION: "🟢",
            MarketState.DISTRIBUTION: "🔴"
        }.get(signal.market_state, "⚪")
        
        current_price = signal.current_price
        entry_mid = (signal.entry_zone[0] + signal.entry_zone[1]) / 2
        tp = signal.take_profit_liquidity
        
        if signal.direction == "LONG":
            risk_pct = (entry_mid - signal.entry_zone[0]) / entry_mid * 100
            reward_pct = (tp - entry_mid) / entry_mid * 100
        else:
            risk_pct = (signal.entry_zone[1] - entry_mid) / entry_mid * 100
            reward_pct = (entry_mid - tp) / entry_mid * 100
        
        rr_ratio = reward_pct / risk_pct if risk_pct > 0 else 0
        
        msg = f"""
{emoji}{state_emoji} <b>ROMEOTPT v4.0 - PURE LIQUIDITY SIGNAL</b>

<b>🎯 {signal.asset}</b> | {signal.direction}
<b>State:</b> {signal.market_state.value}
<b>Setup:</b> {signal.setup_type.value if signal.setup_type else 'NONE'}

<b>📍 Entry Zone:</b> {signal.entry_zone[0]:.8f} - {signal.entry_zone[1]:.8f}
<b>🎯 Take Profit:</b> {signal.take_profit_liquidity:.8f}
<b>📊 Entry TF:</b> {signal.entry_tf}

<b>📈 Signal Score:</b> {signal.signal_score:.2f}/1.0
<b>⚖️  Approx RR:</b> {rr_ratio:.2f}:1 (R:{reward_pct:.1f}% / S:{risk_pct:.1f}%)

<b>🧠 Reason:</b>
{signal.reason}

<b>💧 Liquidity Events:</b>
"""
        
        for i, liq in enumerate(signal.liquidity_taken, 1):
            msg += f"{i}. {liq['type']} at {liq['level']:.8f} ({liq['timeframe']})\n"
        
        msg += f"\n<i>Detected: {datetime.datetime.utcnow().strftime('%H:%M:%S UTC')}</i>"
        
        await send_telegram(msg)
        await send_telegram(f"<code>{output}</code>")
        
    except Exception as e:
        log.error(f"Error sending ROMEOTPT alert: {e}")

async def send_telegram(msg: str, parse_mode="HTML"):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram credentials not set")
        return
    
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            await client.post(url, json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": msg,
                "parse_mode": parse_mode
            })
        except Exception as e:
            log.warning(f"Telegram send failed: {e}")

# ---------------- MAIN SCANNER ----------------
async def romeopt_scanner_main():
    engine = ROMEOTPTEngine()
    tracker = ROMEOTPTSignalTracker()
    
    exchange = ccxt.okx({
        "enableRateLimit": True,
        "options": {"defaultType": "spot"},
        "rateLimit": 10,
        "timeout": 5000,
    })
    
    startup_msg = f"""
🚀 <b>ROMEOTPT v4.0 STARTED</b>
<i>Pure Liquidity & Market State Engine</i>

<b>Core Philosophy:</b>
1. Liquidity is the cause
2. Displacement confirms intent  
3. Market state controls setup
4. Entry is the final step
5. One trade = one liquidity target

<b>Settings:</b>
• Scan: {SCAN_INTERVAL}s
• Top: {TOP_N} symbols
• Min Sweep: {MIN_SWEEP_STRENGTH}
• Entry Distance: {MAX_ENTRY_DISTANCE_PCT}%
"""
    await send_telegram(startup_msg)
    
    scan_cycle = 0
    
    while True:
        scan_cycle += 1
        
        try:
            tickers = await exchange.fetch_tickers()
            usdt_pairs = []
            
            for symbol, data in tickers.items():
                if symbol.endswith("/USDT") and "USDC" not in symbol:
                    volume = data.get("quoteVolume", 0)
                    if isinstance(volume, (int, float)) and volume > 0:
                        usdt_pairs.append((symbol, float(volume)))
            
            usdt_pairs.sort(key=lambda x: x[1], reverse=True)
            symbols_to_scan = [s[0] for s in usdt_pairs[:TOP_N]]
            
            log.info(f"🔄 Scan #{scan_cycle}: {len(symbols_to_scan)} symbols | Active: {len(tracker.active_signals)}")
            
            signals_found = 0
            tasks = []
            
            for symbol in symbols_to_scan:
                task = asyncio.create_task(engine.generate_signal(exchange, symbol))
                tasks.append(task)
                
                if len(tasks) >= MAX_CONCURRENT:
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    for result in results:
                        if isinstance(result, Exception):
                            log.error(f"Task error: {result}")
                            continue
                        
                        if result and result.is_valid():
                            should_alert, reason = tracker.should_alert(result)
                            
                            if should_alert:
                                await send_romeopt_alert(result)
                                tracker.update_signal(result, alerted=True)
                                signals_found += 1
                                log.info(f"📨 Alert sent for {result.asset}: {reason}")
                            else:
                                tracker.update_signal(result, alerted=False)
                                log.debug(f"⏸️  Skipped {result.asset}: {reason}")
                    
                    tasks = []
            
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for result in results:
                    if isinstance(result, Exception):
                        continue
                    
                    if result and result.is_valid():
                        should_alert, reason = tracker.should_alert(result)
                        
                        if should_alert:
                            await send_romeopt_alert(result)
                            tracker.update_signal(result, alerted=True)
                            signals_found += 1
            
            if scan_cycle % 10 == 0:
                tracker.cleanup_old_signals()
            
            await asyncio.sleep(SCAN_INTERVAL)
            
        except Exception as e:
            log.error(f"Scanner error: {e}")
            await asyncio.sleep(SCAN_INTERVAL * 2)

# ---------------- FASTAPI ----------------
app = FastAPI()

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "version": "4.0",
        "philosophy": "Pure ROMEOTPT - Liquidity & Market State"
    }

@app.get("/signals/active")
async def get_active_signals():
    return {"message": "Active signals available in memory"}

# ---------------- MAIN ----------------
async def main():
    try:
        log.info("🚀 ROMEOTPT v4.0 - PURE LIQUIDITY ENGINE")
        log.info("Core: Liquidity → Market State → Setup → Entry")
        log.info(f"Scan: {SCAN_INTERVAL}s | Top {TOP_N} symbols")
        
        await romeopt_scanner_main()
        
    except KeyboardInterrupt:
        log.info("Scanner stopped by user")
    except Exception as e:
        log.error(f"Fatal error: {e}")

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