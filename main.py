#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ROMEOTPT v4.0 - PURE LIQUIDITY & MARKET STATE ENGINE
Implementation of true ROMEOTPT philosophy
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
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from enum import Enum

# ---------------- CONFIG ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
DB_PATH = os.getenv("DB_PATH", "/app/data/romeopt_v4_0.db")

# Scanner settings
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 60))  # 1 minute for proper liquidity analysis
TOP_N = int(os.getenv("TOP_N", 30))
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", 8))

# ROMEOTPT-specific settings
MIN_SWEEP_STRENGTH = float(os.getenv("MIN_SWEEP_STRENGTH", 0.8))
MIN_DISPLACEMENT_RATIO = float(os.getenv("MIN_DISPLACEMENT_RATIO", 1.5))
MAX_ENTRY_DISTANCE_PCT = float(os.getenv("MAX_ENTRY_DISTANCE_PCT", 1.0))

# Deduplication settings
SIGNAL_COOLDOWN_MINUTES = int(os.getenv("SIGNAL_COOLDOWN_MINUTES", 30))  # Longer cooldown
SIGNAL_VALIDITY_HOURS = int(os.getenv("SIGNAL_VALIDITY_HOURS", 4))

# ---------------- ENUMS ----------------
class MarketState(Enum):
    ACCUMULATION = "ACCUMULATION"
    EXPANSION = "EXPANSION"
    DISTRIBUTION = "DISTRIBUTION"
    UNCLEAR = "UNCLEAR"

class SetupType(Enum):
    SWEEP_REVERSAL = "SWEEP_REVERSAL"  # For Accumulation
    PULLBACK_CONTINUATION = "PULLBACK_CONTINUATION"  # For Expansion
    FAILED_BREAKOUT = "FAILED_BREAKOUT"  # For Distribution

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
    event_type: str = ""  # "EQUAL_HIGH", "EQUAL_LOW", "RANGE_SWEEP", "SWING_LIQUIDITY"
    timeframe: str = ""
    is_taken: bool = False
    close_back_inside: bool = False
    displacement_away: bool = False
    strength: float = 0.0  # 0-1.0
    
    def is_valid(self) -> bool:
        return self.is_taken and (self.close_back_inside or self.displacement_away)

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
    candle_body_ratio: float = 0.0  # body_size / total_range
    closes_through_structure: bool = False
    leaves_inefficiency: bool = False
    displacement_direction: str = ""  # "BULLISH", "BEARISH"
    
    def is_valid(self) -> bool:
        return (self.has_displacement and 
                self.candle_body_ratio >= 0.6 and  # Strong real body
                self.closes_through_structure and
                self.leaves_inefficiency)

@dataclass
class StructureShift:
    """STEP 5: MARKET STRUCTURE SHIFT"""
    has_shift: bool = False
    shift_type: str = ""  # "BULLISH_BREAK", "BEARISH_BREAK"
    broken_level: float = 0.0
    confirmation_price: float = 0.0
    
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
    
    def to_output_format(self) -> str:
        """Output in the mandatory format"""
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
        """Check if signal meets all ROMEOTPT rules"""
        return all([
            self.asset,
            self.direction in ["LONG", "SHORT"],
            self.market_state != MarketState.UNCLEAR,
            self.setup_type is not None,
            self.take_profit_liquidity > 0,
            self.entry_zone[0] > 0,
            self.signal_score >= 0.7
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
            # Get 4H data for range analysis
            ohlcv_4h = await self._fetch_ohlcv_with_timeout(exchange, symbol, "4h", 100)
            if not ohlcv_4h or len(ohlcv_4h) < 50:
                return narrative
            
            df_4h = pd.DataFrame(ohlcv_4h, columns=["timestamp", "open", "high", "low", "close", "volume"])
            
            # Determine market type: RANGE or TREND
            recent_high = df_4h['high'].iloc[-20:].max()
            recent_low = df_4h['low'].iloc[-20:].min()
            range_height = (recent_high - recent_low) / recent_low * 100
            
            current_price = df_4h['close'].iloc[-1]
            
            if range_height < 8.0:  # Less than 8% range = RANGE market
                narrative.market_type = "RANGE"
                narrative.range_high = recent_high
                narrative.range_low = recent_low
                
                # Calculate zones
                range_mid = (recent_high + recent_low) / 2
                range_quarter = (recent_high - recent_low) / 4
                
                narrative.premium_zone = (recent_high - range_quarter, recent_high)
                narrative.discount_zone = (recent_low, recent_low + range_quarter)
                narrative.mid_range = (range_mid - range_quarter, range_mid + range_quarter)
                
                # Check if price is at extreme
                if current_price >= narrative.premium_zone[0] or current_price <= narrative.discount_zone[1]:
                    narrative.is_at_extreme = True
                else:
                    narrative.is_at_extreme = False
            else:
                narrative.market_type = "TREND"
                # For trends, use recent swings
                swing_highs = self._find_swing_highs(df_4h)
                swing_lows = self._find_swing_lows(df_4h)
                
                if swing_highs:
                    narrative.range_high = max(swing_highs[-3:]) if len(swing_highs) >= 3 else swing_highs[-1]
                if swing_lows:
                    narrative.range_low = min(swing_lows[-3:]) if len(swing_lows) >= 3 else swing_lows[-1]
                
                # In trends, premium/discount relative to recent swings
                narrative.premium_zone = (narrative.range_high * 0.98, narrative.range_high)
                narrative.discount_zone = (narrative.range_low, narrative.range_low * 1.02)
                narrative.mid_range = (narrative.range_low * 1.02, narrative.range_high * 0.98)
                narrative.is_at_extreme = False  # Different logic for trends
            
            # Find external liquidity levels (swing highs/lows)
            narrative.external_liquidity_levels = self._find_external_liquidity(df_4h)
            
        except Exception as e:
            self.log.debug(f"HTF narrative error for {symbol}: {e}")
        
        return narrative
    
    async def detect_liquidity_events(self, exchange, symbol: str, narrative: HTFNarrative) -> List[LiquidityEvent]:
        """STEP 2: LIQUIDITY ENGINE - Detect REAL stop-taking"""
        events = []
        
        try:
            # Check multiple timeframes
            timeframes = ["1h", "30m", "15m"]
            
            for tf in timeframes:
                ohlcv = await self._fetch_ohlcv_with_timeout(exchange, symbol, tf, 50)
                if not ohlcv or len(ohlcv) < 20:
                    continue
                
                df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
                current_candle = df.iloc[-1]
                prev_candle = df.iloc[-2] if len(df) >= 2 else None
                
                # 1. Check equal highs/lows (last 5 candles)
                for i in range(-6, -1):
                    if abs(i) > len(df):
                        continue
                    
                    candle = df.iloc[i]
                    # Equal highs
                    high_neighbors = df['high'].iloc[max(i-2, 0):min(i+3, len(df))]
                    if candle['high'] == high_neighbors.max() and high_neighbors.value_counts().iloc[0] >= 2:
                        event = LiquidityEvent(
                            level=candle['high'],
                            event_type="EQUAL_HIGH",
                            timeframe=tf,
                            is_taken=current_candle['high'] > candle['high'],
                            close_back_inside=current_candle['close'] < candle['high'] if prev_candle else False,
                            displacement_away=current_candle['close'] < prev_candle['low'] if prev_candle else False
                        )
                        if event.is_valid():
                            event.strength = self._calculate_sweep_strength(df, i, "HIGH")
                            events.append(event)
                    
                    # Equal lows
                    low_neighbors = df['low'].iloc[max(i-2, 0):min(i+3, len(df))]
                    if candle['low'] == low_neighbors.min() and low_neighbors.value_counts().iloc[0] >= 2:
                        event = LiquidityEvent(
                            level=candle['low'],
                            event_type="EQUAL_LOW",
                            timeframe=tf,
                            is_taken=current_candle['low'] < candle['low'],
                            close_back_inside=current_candle['close'] > candle['low'] if prev_candle else False,
                            displacement_away=current_candle['close'] > prev_candle['high'] if prev_candle else False
                        )
                        if event.is_valid():
                            event.strength = self._calculate_sweep_strength(df, i, "LOW")
                            events.append(event)
                
                # 2. Check range high/low sweep (relative to HTF narrative)
                if narrative.range_high > 0 and current_candle['high'] > narrative.range_high:
                    event = LiquidityEvent(
                        level=narrative.range_high,
                        event_type="RANGE_SWEEP",
                        timeframe=tf,
                        is_taken=True,
                        close_back_inside=current_candle['close'] < narrative.range_high,
                        displacement_away=current_candle['close'] < prev_candle['low'] if prev_candle else False
                    )
                    if event.is_valid():
                        events.append(event)
                
                if narrative.range_low > 0 and current_candle['low'] < narrative.range_low:
                    event = LiquidityEvent(
                        level=narrative.range_low,
                        event_type="RANGE_SWEEP",
                        timeframe=tf,
                        is_taken=True,
                        close_back_inside=current_candle['close'] > narrative.range_low,
                        displacement_away=current_candle['close'] > prev_candle['high'] if prev_candle else False
                    )
                    if event.is_valid():
                        events.append(event)
            
            # Filter for strongest events only
            if events:
                strongest_event = max(events, key=lambda x: x.strength)
                events = [strongest_event]  # Only take the strongest liquidity event
        
        except Exception as e:
            self.log.debug(f"Liquidity detection error for {symbol}: {e}")
        
        return events
    
    async def determine_market_state(self, exchange, symbol: str, narrative: HTFNarrative) -> MarketStateAnalysis:
        """STEP 3: MARKET STATE ENGINE"""
        analysis = MarketStateAnalysis()
        
        try:
            # Get 1H data for state analysis
            ohlcv_1h = await self._fetch_ohlcv_with_timeout(exchange, symbol, "1h", 100)
            if not ohlcv_1h or len(ohlcv_1h) < 30:
                analysis.state = MarketState.UNCLEAR
                return analysis
            
            df_1h = pd.DataFrame(ohlcv_1h, columns=["timestamp", "open", "high", "low", "close", "volume"])
            
            # Calculate basic metrics
            recent_high = df_1h['high'].iloc[-10:].max()
            recent_low = df_1h['low'].iloc[-10:].min()
            current_price = df_1h['close'].iloc[-1]
            
            # Check for ACCUMULATION (repeated sweeps, no sustained displacement)
            sweep_count = 0
            for i in range(-15, -1):
                if abs(i) > len(df_1h) - 1:
                    continue
                
                candle = df_1h.iloc[i]
                next_candle = df_1h.iloc[i+1]
                
                # Check for sweep and rejection
                if (candle['high'] > df_1h['high'].iloc[max(i-5, 0):i].max() and 
                    next_candle['close'] < candle['high']):
                    sweep_count += 1
                elif (candle['low'] < df_1h['low'].iloc[max(i-5, 0):i].min() and 
                      next_candle['close'] > candle['low']):
                    sweep_count += 1
            
            if sweep_count >= 2 and (recent_high - recent_low) / recent_low < 0.05:
                analysis.state = MarketState.ACCUMULATION
                analysis.confidence = min(0.8, sweep_count / 4)
                analysis.reasons.append(f"Multiple sweeps detected ({sweep_count})")
                analysis.reasons.append("Price contained in tight range")
                return analysis
            
            # Check for EXPANSION (strong displacement, clean directional closes)
            directional_bars = 0
            for i in range(-8, 0):
                if abs(i) >= len(df_1h):
                    continue
                
                candle = df_1h.iloc[i]
                body_size = abs(candle['close'] - candle['open'])
                total_range = candle['high'] - candle['low']
                
                if body_size / total_range > 0.7:  # Strong real body
                    if candle['close'] > candle['open']:
                        directional_bars += 1
                    else:
                        directional_bars -= 1
            
            if abs(directional_bars) >= 5:
                analysis.state = MarketState.EXPANSION
                analysis.confidence = min(0.9, abs(directional_bars) / 8)
                direction = "BULLISH" if directional_bars > 0 else "BEARISH"
                analysis.reasons.append(f"Strong {direction} expansion")
                analysis.reasons.append(f"{abs(directional_bars)} strong directional bars")
                return analysis
            
            # Check for DISTRIBUTION (prior trend exists, momentum weakening)
            # Get 4H trend context
            ohlcv_4h = await self._fetch_ohlcv_with_timeout(exchange, symbol, "4h", 30)
            if ohlcv_4h:
                df_4h = pd.DataFrame(ohlcv_4h, columns=["timestamp", "open", "high", "low", "close", "volume"])
                
                # Simple trend detection
                early_close = df_4h['close'].iloc[-10]
                late_close = df_4h['close'].iloc[-1]
                trend_direction = "UP" if late_close > early_close * 1.03 else "DOWN" if late_close < early_close * 0.97 else "SIDEWAYS"
                
                if trend_direction != "SIDEWAYS":
                    # Check for failed continuation
                    recent_momentum = self._calculate_momentum(df_1h)
                    if abs(recent_momentum) < 0.5:  # Momentum weakening
                        analysis.state = MarketState.DISTRIBUTION
                        analysis.confidence = 0.7
                        analysis.reasons.append(f"Prior {trend_direction} trend exists")
                        analysis.reasons.append("Momentum weakening")
                        return analysis
            
            analysis.state = MarketState.UNCLEAR
            analysis.confidence = 0.3
            
        except Exception as e:
            self.log.debug(f"Market state error for {symbol}: {e}")
            analysis.state = MarketState.UNCLEAR
        
        return analysis
    
    async def analyze_displacement(self, exchange, symbol: str, direction: str) -> DisplacementAnalysis:
        """STEP 5.2: DISPLACEMENT ANALYSIS"""
        analysis = DisplacementAnalysis()
        
        try:
            # Use 15m for displacement analysis
            ohlcv_15m = await self._fetch_ohlcv_with_timeout(exchange, symbol, "15m", 10)
            if not ohlcv_15m or len(ohlcv_15m) < 3:
                return analysis
            
            df_15m = pd.DataFrame(ohlcv_15m, columns=["timestamp", "open", "high", "low", "close", "volume"])
            
            # Get the displacement candle (should be recent)
            for i in range(-3, 0):
                if abs(i) >= len(df_15m):
                    continue
                
                candle = df_15m.iloc[i]
                body_size = abs(candle['close'] - candle['open'])
                total_range = candle['high'] - candle['low']
                
                if body_size / total_range >= 0.6:  # Strong real body
                    analysis.has_displacement = True
                    analysis.candle_body_ratio = body_size / total_range
                    analysis.displacement_direction = "BULLISH" if candle['close'] > candle['open'] else "BEARISH"
                    
                    # Check if closes through minor structure
                    if i < -1:
                        prev_candle = df_15m.iloc[i-1]
                        if analysis.displacement_direction == "BULLISH":
                            analysis.closes_through_structure = candle['close'] > prev_candle['high']
                            # Leaves inefficiency (gap)
                            analysis.leaves_inefficiency = candle['low'] > prev_candle['high']
                        else:
                            analysis.closes_through_structure = candle['close'] < prev_candle['low']
                            analysis.leaves_inefficiency = candle['high'] < prev_candle['low']
                    
                    break
        
        except Exception as e:
            self.log.debug(f"Displacement analysis error for {symbol}: {e}")
        
        return analysis
    
    async def analyze_structure_shift(self, exchange, symbol: str, direction: str) -> StructureShift:
        """STEP 5.3: MARKET STRUCTURE SHIFT"""
        shift = StructureShift()
        
        try:
            # Use 5m for structure shift detection
            ohlcv_5m = await self._fetch_ohlcv_with_timeout(exchange, symbol, "5m", 20)
            if not ohlcv_5m or len(ohlcv_5m) < 10:
                return shift
            
            df_5m = pd.DataFrame(ohlcv_5m, columns=["timestamp", "open", "high", "low", "close", "volume"])
            
            # Find swing highs and lows
            swing_highs = []
            swing_lows = []
            
            for i in range(1, len(df_5m) - 1):
                if (df_5m['high'].iloc[i] > df_5m['high'].iloc[i-1] and 
                    df_5m['high'].iloc[i] > df_5m['high'].iloc[i+1]):
                    swing_highs.append((i, df_5m['high'].iloc[i]))
                
                if (df_5m['low'].iloc[i] < df_5m['low'].iloc[i-1] and 
                    df_5m['low'].iloc[i] < df_5m['low'].iloc[i+1]):
                    swing_lows.append((i, df_5m['low'].iloc[i]))
            
            # Check for structure break
            current_price = df_5m['close'].iloc[-1]
            
            if direction == "LONG":
                # Need to break a lower high
                if len(swing_highs) >= 2:
                    recent_swing_high = swing_highs[-1][1]
                    prev_swing_high = swing_highs[-2][1] if len(swing_highs) >= 2 else 0
                    
                    if prev_swing_high > recent_swing_high:  # Lower high exists
                        shift.has_shift = current_price > recent_swing_high
                        shift.shift_type = "BULLISH_BREAK"
                        shift.broken_level = recent_swing_high
                        shift.confirmation_price = current_price
            
            else:  # SHORT
                # Need to break a higher low
                if len(swing_lows) >= 2:
                    recent_swing_low = swing_lows[-1][1]
                    prev_swing_low = swing_lows[-2][1] if len(swing_lows) >= 2 else float('inf')
                    
                    if prev_swing_low < recent_swing_low:  # Higher low exists
                        shift.has_shift = current_price < recent_swing_low
                        shift.shift_type = "BEARISH_BREAK"
                        shift.broken_level = recent_swing_low
                        shift.confirmation_price = current_price
        
        except Exception as e:
            self.log.debug(f"Structure shift error for {symbol}: {e}")
        
        return shift
    
    async def find_take_profit_liquidity(self, exchange, symbol: str, direction: str, 
                                       narrative: HTFNarrative, entry_tf: str) -> float:
        """STEP 6: TAKE PROFIT ENGINE - Find untouched external liquidity"""
        
        # Rule: TP liquidity must be on higher TF than entry
        higher_tf_map = {
            "5m": ["15m", "30m", "1h"],
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
                    # Find untouched swing highs (liquidity above)
                    swing_highs = self._find_swing_highs(df)
                    valid_highs = []
                    
                    for high in swing_highs[-5:]:  # Recent swings
                        if high > current_price * 1.005:  # Must be above current price
                            # Check if it's been taken recently
                            recent_max = df['high'].iloc[-5:].max()
                            if high > recent_max:  # Untouched
                                valid_highs.append(high)
                    
                    if valid_highs:
                        # Take the closest valid liquidity
                        return min(valid_highs)
                
                else:  # SHORT
                    # Find untouched swing lows (liquidity below)
                    swing_lows = self._find_swing_lows(df)
                    valid_lows = []
                    
                    for low in swing_lows[-5:]:
                        if low < current_price * 0.995:  # Must be below current price
                            recent_min = df['low'].iloc[-5:].min()
                            if low < recent_min:  # Untouched
                                valid_lows.append(low)
                    
                    if valid_lows:
                        # Take the closest valid liquidity
                        return max(valid_lows)
        
        except Exception as e:
            self.log.debug(f"TP liquidity error for {symbol}: {e}")
        
        # Fallback: Use HTF narrative levels
        if direction == "LONG" and narrative.external_liquidity_levels:
            for level in sorted(narrative.external_liquidity_levels):
                if level > current_price * 1.01:
                    return level
        elif direction == "SHORT" and narrative.external_liquidity_levels:
            for level in sorted(narrative.external_liquidity_levels, reverse=True):
                if level < current_price * 0.99:
                    return level
        
        return 0.0
    
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
                if (narrative.mid_range[0] <= current_price <= narrative.mid_range[1]):
                    self.log.debug(f"{symbol}: Price in mid-range, no trade")
                    return None
            
            # === STEP 2: LIQUIDITY DETECTION ===
            liquidity_events = await self.detect_liquidity_events(exchange, symbol, narrative)
            signal.liquidity_taken = [{
                "level": e.level,
                "type": e.event_type,
                "timeframe": e.timeframe,
                "strength": e.strength
            } for e in liquidity_events]
            
            # Rule: If no external liquidity taken → STOP
            if not liquidity_events:
                self.log.debug(f"{symbol}: No valid liquidity events")
                return None
            
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
                direction = "LONG" if strongest_event.event_type in ["EQUAL_LOW", "RANGE_SWEEP"] else "SHORT"
            elif market_state.state == MarketState.EXPANSION:
                setup_type = SetupType.PULLBACK_CONTINUATION
                # Need additional logic for expansion direction
                direction = await self._determine_expansion_direction(exchange, symbol)
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
            displacement = await self.analyze_displacement(exchange, symbol, direction)
            if not displacement.is_valid():
                self.log.debug(f"{symbol}: No valid displacement")
                return None
            
            # 5.3 Market Structure Shift
            structure_shift = await self.analyze_structure_shift(exchange, symbol, direction)
            if not structure_shift.is_valid():
                self.log.debug(f"{symbol}: No structure shift")
                return None
            
            # 5.4 Entry Location
            current_price = await self._get_current_price(exchange, symbol)
            signal.current_price = current_price
            
            if direction == "LONG":
                # Longs ONLY in discount
                if not (narrative.discount_zone[0] <= current_price <= narrative.discount_zone[1]):
                    self.log.debug(f"{symbol}: Long not in discount zone")
                    return None
                entry_zone = (
                    current_price * (1 - MAX_ENTRY_DISTANCE_PCT/100),
                    current_price * (1 + MAX_ENTRY_DISTANCE_PCT/100)
                )
                entry_tf = "15m"  # Execution TF
            else:  # SHORT
                # Shorts ONLY in premium
                if not (narrative.premium_zone[0] <= current_price <= narrative.premium_zone[1]):
                    self.log.debug(f"{symbol}: Short not in premium zone")
                    return None
                entry_zone = (
                    current_price * (1 - MAX_ENTRY_DISTANCE_PCT/100),
                    current_price * (1 + MAX_ENTRY_DISTANCE_PCT/100)
                )
                entry_tf = "15m"  # Execution TF
            
            signal.entry_zone = entry_zone
            signal.entry_tf = entry_tf
            
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
                1.0 if structure_shift.has_shift else 0.0
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
        """Fetch OHLCV with timeout"""
        try:
            return await asyncio.wait_for(
                exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit),
                timeout=5.0
            )
        except Exception as e:
            self.log.debug(f"Failed to fetch {symbol} {timeframe}: {e}")
            return None
    
    async def _get_current_price(self, exchange, symbol: str) -> float:
        """Get current price"""
        try:
            ticker = await exchange.fetch_ticker(symbol)
            return ticker.get('last', 0)
        except:
            return 0
    
    async def _determine_expansion_direction(self, exchange, symbol: str) -> str:
        """Determine expansion direction from 1H data"""
        try:
            ohlcv_1h = await self._fetch_ohlcv_with_timeout(exchange, symbol, "1h", 10)
            if not ohlcv_1h:
                return "LONG"  # Default
            
            df_1h = pd.DataFrame(ohlcv_1h, columns=["timestamp", "open", "high", "low", "close", "volume"])
            
            bullish_bars = 0
            for i in range(-5, 0):
                if abs(i) >= len(df_1h):
                    continue
                candle = df_1h.iloc[i]
                if candle['close'] > candle['open']:
                    bullish_bars += 1
            
            return "LONG" if bullish_bars >= 3 else "SHORT"
        except:
            return "LONG"
    
    def _find_swing_highs(self, df: pd.DataFrame, lookback: int = 3) -> List[float]:
        """Find swing highs in dataframe"""
        highs = []
        for i in range(lookback, len(df) - lookback):
            if df['high'].iloc[i] == df['high'].iloc[i-lookback:i+lookback+1].max():
                highs.append(df['high'].iloc[i])
        return highs
    
    def _find_swing_lows(self, df: pd.DataFrame, lookback: int = 3) -> List[float]:
        """Find swing lows in dataframe"""
        lows = []
        for i in range(lookback, len(df) - lookback):
            if df['low'].iloc[i] == df['low'].iloc[i-lookback:i+lookback+1].min():
                lows.append(df['low'].iloc[i])
        return lows
    
    def _find_external_liquidity(self, df: pd.DataFrame) -> List[float]:
        """Find external liquidity levels (significant swing points)"""
        levels = []
        
        # Find significant swing highs
        swing_highs = self._find_swing_highs(df, 5)
        for high in swing_highs[-10:]:
            levels.append(high)
        
        # Find significant swing lows
        swing_lows = self._find_swing_lows(df, 5)
        for low in swing_lows[-10:]:
            levels.append(low)
        
        return sorted(set(levels))
    
    def _calculate_sweep_strength(self, df: pd.DataFrame, sweep_idx: int, sweep_type: str) -> float:
        """Calculate strength of a liquidity sweep (0-1)"""
        if sweep_idx >= len(df) - 1:
            return 0.5
        
        sweep_candle = df.iloc[sweep_idx]
        next_candle = df.iloc[sweep_idx + 1]
        
        strength = 0.5
        
        # 1. Wick size relative to body
        if sweep_type == "HIGH":
            wick_size = sweep_candle['high'] - max(sweep_candle['open'], sweep_candle['close'])
            body_size = abs(sweep_candle['close'] - sweep_candle['open'])
            if body_size > 0:
                wick_ratio = wick_size / body_size
                if wick_ratio > 1.5:
                    strength += 0.2  # Long wick = strong liquidity
        else:  # LOW
            wick_size = min(sweep_candle['open'], sweep_candle['close']) - sweep_candle['low']
            body_size = abs(sweep_candle['close'] - sweep_candle['open'])
            if body_size > 0:
                wick_ratio = wick_size / body_size
                if wick_ratio > 1.5:
                    strength += 0.2
        
        # 2. Next candle reaction
        if sweep_type == "HIGH":
            if next_candle['close'] < sweep_candle['close']:
                strength += 0.3
        else:
            if next_candle['close'] > sweep_candle['close']:
                strength += 0.3
        
        return min(strength, 1.0)
    
    def _calculate_momentum(self, df: pd.DataFrame, period: int = 5) -> float:
        """Calculate price momentum"""
        if len(df) < period + 1:
            return 0.0
        
        early_close = df['close'].iloc[-period-1]
        late_close = df['close'].iloc[-1]
        
        return (late_close - early_close) / early_close
    
    def _calculate_signal_score(self, sweep_strength: float, state_confidence: float,
                              displacement_ratio: float, structure_score: float) -> float:
        """Calculate overall signal score (0-1)"""
        weights = [0.3, 0.25, 0.25, 0.2]  # Liquidity, State, Displacement, Structure
        scores = [sweep_strength, state_confidence, displacement_ratio, structure_score]
        
        return sum(w * s for w, s in zip(weights, scores))

# ---------------- SIGNAL TRACKER (UPDATED) ----------------
class ROMEOTPTSignalTracker:
    """Signal tracker for ROMEOTPT signals"""
    
    def __init__(self):
        self.active_signals = {}  # symbol -> ROMEOTPTSignal
        self.signal_history = []
    
    def should_alert(self, signal: ROMEOTPTSignal) -> Tuple[bool, str]:
        """Check if we should alert for this signal"""
        symbol = signal.asset
        
        if symbol not in self.active_signals:
            return True, "New signal"
        
        old_signal = self.active_signals[symbol]
        
        # Check if old signal expired
        old_time = datetime.datetime.fromisoformat(old_signal.timestamp)
        new_time = datetime.datetime.fromisoformat(signal.timestamp)
        age_hours = (new_time - old_time).total_seconds() / 3600
        
        if age_hours > SIGNAL_VALIDITY_HOURS:
            self.remove_signal(symbol)
            return True, "Old signal expired"
        
        # Check if same setup
        if old_signal.setup_type != signal.setup_type:
            return True, "Setup type changed"
        
        # Check if same direction
        if old_signal.direction != signal.direction:
            return True, "Direction changed"
        
        # Check if TP level changed significantly
        tp_diff = abs(old_signal.take_profit_liquidity - signal.take_profit_liquidity)
        tp_diff_pct = tp_diff / old_signal.take_profit_liquidity * 100 if old_signal.take_profit_liquidity > 0 else 0
        
        if tp_diff_pct > 2.0:  # TP moved > 2%
            return True, f"TP moved {tp_diff_pct:.1f}%"
        
        # Check if score improved
        if signal.signal_score - old_signal.signal_score > 0.1:
            return True, f"Score improved {old_signal.signal_score:.2f}→{signal.signal_score:.2f}"
        
        # Check cooldown
        last_alerted = self.active_signals[symbol].get('last_alerted')
        if last_alerted:
            time_since_alert = (new_time - last_alerted).total_seconds() / 60
            if time_since_alert < SIGNAL_COOLDOWN_MINUTES:
                return False, f"In cooldown ({int(SIGNAL_COOLDOWN_MINUTES - time_since_alert)}min left)"
        
        return False, "Similar signal active"
    
    def update_signal(self, signal: ROMEOTPTSignal, alerted: bool = False):
        """Update or add signal"""
        symbol = signal.asset
        
        signal_dict = {
            'signal': signal,
            'first_seen': datetime.datetime.fromisoformat(signal.timestamp),
            'alert_count': 0,
            'status': 'active'
        }
        
        if alerted:
            signal_dict['last_alerted'] = datetime.datetime.utcnow()
            signal_dict['alert_count'] = 1
        
        self.active_signals[symbol] = signal_dict
    
    def remove_signal(self, symbol: str):
        """Remove signal from active tracking"""
        if symbol in self.active_signals:
            signal_data = self.active_signals.pop(symbol)
            signal_data['status'] = 'expired'
            signal_data['expired_at'] = datetime.datetime.utcnow()
            self.signal_history.append(signal_data)

# ---------------- TELEGRAM OUTPUT ----------------
async def send_romeopt_alert(signal: ROMEOTPTSignal):
    """Send ROMEOTPT signal in the mandatory format"""
    try:
        # Get signal details
        output = signal.to_output_format()
        
        # Format for Telegram
        emoji = "🟢" if signal.direction == "LONG" else "🔴"
        state_emoji = {
            MarketState.ACCUMULATION: "🟡",
            MarketState.EXPANSION: "🟢",
            MarketState.DISTRIBUTION: "🔴"
        }.get(signal.market_state, "⚪")
        
        # Calculate approximate RR
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
        
        # Also send the raw output for verification
        await send_telegram(f"<code>{output}</code>")
        
    except Exception as e:
        log.error(f"Error sending ROMEOTPT alert: {e}")

async def send_telegram(msg: str, parse_mode="HTML"):
    """Send message to Telegram"""
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
    """Main ROMEOTPT scanner"""
    
    # Initialize
    engine = ROMEOTPTEngine()
    tracker = ROMEOTPTSignalTracker()
    
    # Create exchange
    exchange = ccxt.okx({
        "enableRateLimit": True,
        "options": {"defaultType": "spot"},
        "rateLimit": 10,
        "timeout": 5000,
    })
    
    # Startup message
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
            # Get top symbols by volume
            tickers = await exchange.fetch_tickers()
            usdt_pairs = []
            
            for symbol, data in tickers.items():
                if symbol.endswith("/USDT") and "USDC" not in symbol:
                    volume = data.get("quoteVolume", 0)
                    if isinstance(volume, (int, float)) and volume > 0:
                        usdt_pairs.append((symbol, float(volume)))
            
            usdt_pairs.sort(key=lambda x: x[1], reverse=True)
            symbols_to_scan = [s[0] for s in usdt_pairs[:TOP_N]]
            
            log.info(f"🔄 Scan #{scan_cycle}: {len(symbols_to_scan)} symbols")
            
            # Scan symbols
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
                            # Check if we should alert
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
            
            log.info(f"📊 Scan #{scan_cycle} complete: {signals_found} signals found, {len(tracker.active_signals)} active")
            
            # Periodic cleanup
            if scan_cycle % 10 == 0:
                current_time = datetime.datetime.utcnow()
                expired = []
                
                for symbol, data in tracker.active_signals.items():
                    age_hours = (current_time - data['first_seen']).total_seconds() / 3600
                    if age_hours > SIGNAL_VALIDITY_HOURS:
                        expired.append(symbol)
                
                for symbol in expired:
                    tracker.remove_signal(symbol)
                
                if expired:
                    log.info(f"🧹 Cleaned up {len(expired)} expired signals")
            
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
    """Get active ROMEOTPT signals"""
    # This would need the tracker instance
    return {"message": "Active signals endpoint - implement with tracker"}

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