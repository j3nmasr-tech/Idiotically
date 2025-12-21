#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ROMEOTPT v5.0 - INSTITUTIONAL LIQUIDITY & INTENT ENGINE
Pure trader logic, no engineer thinking
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
DB_PATH = os.getenv("DB_PATH", "/app/data/romeopt_v5_0.db")

# Scanner settings
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 60))
TOP_N = int(os.getenv("TOP_N", 60))
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", 6))

# Deduplication
SIGNAL_COOLDOWN_MINUTES = int(os.getenv("SIGNAL_COOLDOWN_MINUTES", 45))
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
log = logging.getLogger("romeopt_v5_0")

# ---------------- BTC NARRATIVE MODULE ----------------
@dataclass
class BTCNarrative:
    """Global BTC narrative that filters all alt signals"""
    bias: str = "NEUTRAL"  # BULLISH, BEARISH, NEUTRAL
    market_state: MarketState = MarketState.UNCLEAR
    htf_range_high: float = 0.0
    htf_range_low: float = 0.0
    is_mid_range: bool = False
    last_update: datetime.datetime = field(default_factory=datetime.datetime.utcnow)
    
    def should_block_alt_signal(self, alt_direction: str) -> bool:
        """RomeOTPT Rule: BTC narrative controls alt behavior"""
        if self.is_mid_range:
            return True  # Block all signals when BTC is mid-range
        
        if self.market_state == MarketState.EXPANSION:
            if self.bias == "BULLISH" and alt_direction == "SHORT":
                return True  # Don't short against BTC bull expansion
            elif self.bias == "BEARISH" and alt_direction == "LONG":
                return True  # Don't long against BTC bear expansion
        
        return False
    
    def to_dict(self):
        return {
            "bias": self.bias,
            "market_state": self.market_state.value,
            "htf_range": (self.htf_range_low, self.htf_range_high),
            "is_mid_range": self.is_mid_range,
            "age_seconds": (datetime.datetime.utcnow() - self.last_update).total_seconds()
        }

class BTCNarrativeEngine:
    """Analyzes BTC for global market context"""
    
    async def analyze(self, exchange) -> BTCNarrative:
        narrative = BTCNarrative()
        
        try:
            # Get 4H data
            ohlcv_4h = await self._fetch_ohlcv(exchange, "BTC/USDT", "4h", 100)
            if not ohlcv_4h or len(ohlcv_4h) < 50:
                return narrative
            
            df_4h = pd.DataFrame(ohlcv_4h, columns=["timestamp", "open", "high", "low", "close", "volume"])
            
            # Find recent range (last 20 candles = ~80 hours)
            recent_high = df_4h['high'].iloc[-20:].max()
            recent_low = df_4h['low'].iloc[-20:].min()
            current_price = df_4h['close'].iloc[-1]
            
            narrative.htf_range_high = recent_high
            narrative.htf_range_low = recent_low
            
            # RomeOTPT: Range vs Trend by BEHAVIOR, not percentage
            # Check for failed displacement attempts
            failed_displacements = 0
            for i in range(-10, -1):
                if abs(i) >= len(df_4h) - 1:
                    continue
                
                candle = df_4h.iloc[i]
                next_candle = df_4h.iloc[i+1]
                
                # Strong candle that fails to continue
                body_ratio = abs(candle['close'] - candle['open']) / (candle['high'] - candle['low']) if (candle['high'] - candle['low']) > 0 else 0
                if body_ratio > 0.7:
                    if candle['close'] > candle['open']:
                        # Bullish candle that gets rejected
                        if next_candle['close'] < candle['close'] and next_candle['close'] < next_candle['open']:
                            failed_displacements += 1
                    else:
                        # Bearish candle that gets rejected
                        if next_candle['close'] > candle['close'] and next_candle['close'] > next_candle['open']:
                            failed_displacements += 1
            
            # Check for liquidity sweeps on both sides
            buy_side_swept = False
            sell_side_swept = False
            
            swing_lows = self._find_swing_lows(df_4h, 3)
            swing_highs = self._find_swing_highs(df_4h, 3)
            
            if swing_lows:
                recent_low_swing = min(swing_lows[-3:]) if len(swing_lows) >= 3 else swing_lows[-1]
                buy_side_swept = current_price > recent_low_swing * 1.02  # Price moved away from low
            
            if swing_highs:
                recent_high_swing = max(swing_highs[-3:]) if len(swing_highs) >= 3 else swing_highs[-1]
                sell_side_swept = current_price < recent_high_swing * 0.98  # Price moved away from high
            
            # Determine market state by INTENT, not statistics
            if failed_displacements >= 2 and (buy_side_swept or sell_side_swept):
                # Both sides hunted but no follow-through = ACCUMULATION
                narrative.market_state = MarketState.ACCUMULATION
                narrative.bias = "NEUTRAL"
            elif failed_displacements == 0 and (buy_side_swept != sell_side_swept):
                # Clean one-sided movement = EXPANSION
                narrative.market_state = MarketState.EXPANSION
                narrative.bias = "BULLISH" if buy_side_swept else "BEARISH"
            elif failed_displacements >= 1 and buy_side_swept and sell_side_swept:
                # Both sides swept with some failure = DISTRIBUTION
                narrative.market_state = MarketState.DISTRIBUTION
                # Distribution bias depends on which side failed last
                last_failed = self._get_last_failed_side(df_4h)
                narrative.bias = "BEARISH" if last_failed == "BULLISH" else "BULLISH"
            else:
                narrative.market_state = MarketState.UNCLEAR
                narrative.bias = "NEUTRAL"
            
            # RomeOTPT: Check if BTC is mid-range (block all signals)
            range_mid = (recent_high + recent_low) / 2
            range_20pct = (recent_high - recent_low) * 0.2
            
            narrative.is_mid_range = (
                abs(current_price - range_mid) < range_20pct and
                narrative.market_state != MarketState.EXPANSION
            )
            
            narrative.last_update = datetime.datetime.utcnow()
            
        except Exception as e:
            log.error(f"BTC narrative error: {e}")
        
        return narrative
    
    async def _fetch_ohlcv(self, exchange, symbol: str, tf: str, limit: int):
        try:
            return await asyncio.wait_for(
                exchange.fetch_ohlcv(symbol, timeframe=tf, limit=limit),
                timeout=5.0
            )
        except:
            return None
    
    def _find_swing_lows(self, df: pd.DataFrame, lookback: int = 3) -> List[float]:
        lows = []
        for i in range(lookback, len(df) - lookback):
            if df['low'].iloc[i] == df['low'].iloc[i-lookback:i+lookback+1].min():
                lows.append(df['low'].iloc[i])
        return lows
    
    def _find_swing_highs(self, df: pd.DataFrame, lookback: int = 3) -> List[float]:
        highs = []
        for i in range(lookback, len(df) - lookback):
            if df['high'].iloc[i] == df['high'].iloc[i-lookback:i+lookback+1].max():
                highs.append(df['high'].iloc[i])
        return highs
    
    def _get_last_failed_side(self, df: pd.DataFrame) -> str:
        """Check which side (bull/bear) failed most recently"""
        for i in range(-5, -1):
            if abs(i) >= len(df) - 1:
                continue
            
            candle = df.iloc[i]
            next_candle = df.iloc[i+1]
            body_ratio = abs(candle['close'] - candle['open']) / (candle['high'] - candle['low']) if (candle['high'] - candle['low']) > 0 else 0
            
            if body_ratio > 0.7:
                if candle['close'] > candle['open'] and next_candle['close'] < candle['close']:
                    return "BULLISH"  # Bullish attempt failed
                elif candle['close'] < candle['open'] and next_candle['close'] > candle['close']:
                    return "BEARISH"  # Bearish attempt failed
        
        return "NEUTRAL"

# ---------------- CORE ROMEOTPT ENGINE (TRADER LOGIC) ----------------
class ROMEOTPTTraderEngine:
    """Pure trader logic - no engineer thinking"""
    
    def __init__(self, btc_narrative: BTCNarrative):
        self.btc_narrative = btc_narrative
        self.log = logging.getLogger("romeopt_trader")
    
    async def analyze_asset(self, exchange, symbol: str) -> Optional[Dict]:
        """Complete RomeOTPT analysis for one asset"""
        
        # RomeOTPT Rule: Skip if BTC blocks it (check early)
        if symbol == "BTC/USDT":
            return await self._analyze_btc_directly(exchange)
        
        # Get current price first
        current_price = await self._get_price(exchange, symbol)
        if current_price == 0:
            return None
        
        try:
            # === STEP 1: HTF NARRATIVE (BEHAVIOR-BASED) ===
            htf_narrative = await self._analyze_htf_behavior(exchange, symbol)
            
            # RomeOTPT: If price is mid-range → STOP
            if htf_narrative.get('is_mid_range', False):
                self.log.debug(f"{symbol}: Mid-range HTF, no trade")
                return None
            
            # === STEP 2: FIND SINGLE STRONGEST LIQUIDITY EVENT ===
            liquidity_event = await self._find_strongest_liquidity(exchange, symbol, htf_narrative)
            if not liquidity_event:
                return None
            
            # === STEP 3: DETERMINE MARKET STATE (BINARY INTENT) ===
            market_state = await self._determine_market_state_intent(exchange, symbol, htf_narrative, liquidity_event)
            if market_state == MarketState.UNCLEAR:
                return None
            
            # === STEP 4: SETUP SELECTION (STATE-DEPENDENT) ===
            setup_type, direction = self._select_setup_by_state(market_state, liquidity_event)
            if not setup_type:
                return None
            
            # === ROMEOGTPT RULE: BTC NARRATIVE FILTER ===
            if self.btc_narrative.should_block_alt_signal(direction):
                self.log.debug(f"{symbol}: Blocked by BTC narrative")
                return None
            
            # === STEP 5: ENTRY LOGIC (4 CONDITIONS) ===
            entry_analysis = await self._analyze_entry_conditions(
                exchange, symbol, direction, liquidity_event, htf_narrative
            )
            if not entry_analysis['valid']:
                return None
            
            # === STEP 6: FIND TP LIQUIDITY ===
            tp_liquidity = await self._find_tp_liquidity(
                exchange, symbol, direction, entry_analysis['entry_tf']
            )
            if tp_liquidity == 0:
                return None
            
            # === FINAL SIGNAL ASSEMBLY ===
            signal = {
                "asset": symbol,
                "direction": direction,
                "market_state": market_state.value,
                "setup_type": setup_type.value,
                "htf_narrative": htf_narrative,
                "liquidity_taken": {
                    "level": liquidity_event['level'],
                    "type": liquidity_event['type'],
                    "timeframe": liquidity_event['timeframe']
                },
                "entry_zone": entry_analysis['entry_zone'],
                "entry_tf": entry_analysis['entry_tf'],
                "take_profit_liquidity": tp_liquidity,
                "current_price": current_price,
                "timestamp": datetime.datetime.utcnow().isoformat(),
                "reason": f"{liquidity_event['type']} at {liquidity_event['level']:.2f} → {direction} {setup_type.value}",
                "btc_context": self.btc_narrative.to_dict()
            }
            
            # RomeOTPT Final Check: Signal must be OBVIOUS
            if not self._is_signal_obvious(signal):
                return None
            
            return signal
            
        except Exception as e:
            self.log.error(f"Analysis error for {symbol}: {e}")
            return None
    
    async def _analyze_htf_behavior(self, exchange, symbol: str) -> Dict:
        """HTF narrative by BEHAVIOR, not percentages"""
        narrative = {
            "market_type": "UNKNOWN",
            "range_high": 0.0,
            "range_low": 0.0,
            "premium_zone": (0.0, 0.0),
            "discount_zone": (0.0, 0.0),
            "is_mid_range": False,
            "external_liquidity": []
        }
        
        try:
            # Use 4H for HTF narrative
            ohlcv = await self._fetch_ohlcv(exchange, symbol, "4h", 50)
            if not ohlcv or len(ohlcv) < 20:
                return narrative
            
            df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
            
            # Find swing points (true range)
            swing_highs = self._find_swing_highs(df, 5)
            swing_lows = self._find_swing_lows(df, 5)
            
            if not swing_highs or not swing_lows:
                return narrative
            
            recent_high = max(swing_highs[-3:]) if len(swing_highs) >= 3 else swing_highs[-1]
            recent_low = min(swing_lows[-3:]) if len(swing_lows) >= 3 else swing_lows[-1]
            
            narrative["range_high"] = recent_high
            narrative["range_low"] = recent_low
            
            # RomeOTPT: Premium/Discount zones based on swing points, not percentages
            premium_start = recent_high * 0.985  # 1.5% below swing high
            discount_end = recent_low * 1.015    # 1.5% above swing low
            
            narrative["premium_zone"] = (premium_start, recent_high)
            narrative["discount_zone"] = (recent_low, discount_end)
            
            current_price = df['close'].iloc[-1]
            
            # RomeOTPT: Mid-range check by position relative to swings
            range_mid = (recent_high + recent_low) / 2
            range_30pct = (recent_high - recent_low) * 0.3
            
            narrative["is_mid_range"] = (
                abs(current_price - range_mid) < range_30pct and
                current_price > discount_end and
                current_price < premium_start
            )
            
            # External liquidity = recent swing points
            narrative["external_liquidity"] = swing_highs[-5:] + swing_lows[-5:]
            
            # Market type by INTENT (not percentage)
            # Check for failed follow-through after swings
            failed_follow = 0
            for i in range(-8, -1):
                if abs(i) >= len(df) - 1:
                    continue
                
                if swing_highs and df['high'].iloc[i] in swing_highs[-3:]:
                    next_candle = df.iloc[i+1]
                    if next_candle['close'] < df['close'].iloc[i]:
                        failed_follow += 1
                
                if swing_lows and df['low'].iloc[i] in swing_lows[-3:]:
                    next_candle = df.iloc[i+1]
                    if next_candle['close'] > df['close'].iloc[i]:
                        failed_follow += 1
            
            if failed_follow >= 2:
                narrative["market_type"] = "RANGE"
            else:
                narrative["market_type"] = "TREND"
            
        except Exception as e:
            self.log.debug(f"HTF behavior analysis error: {e}")
        
        return narrative
    
    async def _find_strongest_liquidity(self, exchange, symbol: str, htf_narrative: Dict) -> Optional[Dict]:
        """Find ONE strongest liquidity event (RomeOTPT: quality > quantity)"""
        
        try:
            # RomeOTPT: Check only relevant TFs
            # HTF liquidity (4H/1H) for cause, execution parent (15m/30m) for confirmation
            timeframes = ["4h", "1h", "30m"]
            all_events = []
            
            for tf in timeframes:
                ohlcv = await self._fetch_ohlcv(exchange, symbol, tf, 30)
                if not ohlcv or len(ohlcv) < 10:
                    continue
                
                df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
                current_candle = df.iloc[-1]
                prev_candle = df.iloc[-2] if len(df) >= 2 else None
                
                # Check for swing high/low sweeps
                swing_highs = self._find_swing_highs(df, 3)
                swing_lows = self._find_swing_lows(df, 3)
                
                for level in swing_highs[-3:]:
                    if current_candle['high'] > level and current_candle['close'] < level:
                        # Valid sweep: traded beyond, closed back inside
                        event = {
                            'level': level,
                            'type': 'SWING_HIGH_SWEEP',
                            'timeframe': tf,
                            'strength': self._calculate_sweep_strength(df, level, 'HIGH'),
                            'index': len(df) - 1
                        }
                        all_events.append(event)
                
                for level in swing_lows[-3:]:
                    if current_candle['low'] < level and current_candle['close'] > level:
                        event = {
                            'level': level,
                            'type': 'SWING_LOW_SWEEP',
                            'timeframe': tf,
                            'strength': self._calculate_sweep_strength(df, level, 'LOW'),
                            'index': len(df) - 1
                        }
                        all_events.append(event)
                
                # Check HTF range sweeps
                range_high = htf_narrative.get('range_high', 0)
                range_low = htf_narrative.get('range_low', 0)
                
                if range_high > 0 and current_candle['high'] > range_high and current_candle['close'] < range_high:
                    event = {
                        'level': range_high,
                        'type': 'RANGE_HIGH_SWEEP',
                        'timeframe': tf,
                        'strength': 0.9,  # High priority
                        'index': len(df) - 1
                    }
                    all_events.append(event)
                
                if range_low > 0 and current_candle['low'] < range_low and current_candle['close'] > range_low:
                    event = {
                        'level': range_low,
                        'type': 'RANGE_LOW_SWEEP',
                        'timeframe': tf,
                        'strength': 0.9,
                        'index': len(df) - 1
                    }
                    all_events.append(event)
            
            if not all_events:
                return None
            
            # RomeOTPT: Take ONLY the strongest event
            strongest = max(all_events, key=lambda x: x['strength'])
            
            # Must meet minimum strength
            if strongest['strength'] < 0.7:
                return None
            
            return strongest
            
        except Exception as e:
            self.log.debug(f"Liquidity finding error: {e}")
            return None
    
    async def _determine_market_state_intent(self, exchange, symbol: str, 
                                           htf_narrative: Dict, liquidity_event: Dict) -> MarketState:
        """Determine market state by BINARY INTENT checks"""
        
        try:
            # Get 1H data for intent analysis
            ohlcv_1h = await self._fetch_ohlcv(exchange, symbol, "1h", 30)
            if not ohlcv_1h or len(ohlcv_1h) < 20:
                return MarketState.UNCLEAR
            
            df_1h = pd.DataFrame(ohlcv_1h, columns=["timestamp", "open", "high", "low", "close", "volume"])
            current_price = df_1h['close'].iloc[-1]
            
            # === BINARY CHECKS (RomeOTPT thinking) ===
            
            # 1. Check for ACCUMULATION: Both sides hunted, no side allowed to run
            buy_side_hunted = False
            sell_side_hunted = False
            
            swing_lows = self._find_swing_lows(df_1h, 3)
            swing_highs = self._find_swing_highs(df_1h, 3)
            
            if swing_lows:
                recent_low = min(swing_lows[-3:]) if len(swing_lows) >= 3 else swing_lows[-1]
                buy_side_hunted = current_price > recent_low * 1.01  # Moved away from low
            
            if swing_highs:
                recent_high = max(swing_highs[-3:]) if len(swing_highs) >= 3 else swing_highs[-1]
                sell_side_hunted = current_price < recent_high * 0.99  # Moved away from high
            
            # Check for failed displacement after hunts
            displacement_failed = False
            for i in range(-5, -1):
                if abs(i) >= len(df_1h) - 1:
                    continue
                
                candle = df_1h.iloc[i]
                next_candle = df_1h.iloc[i+1]
                body_ratio = abs(candle['close'] - candle['open']) / (candle['high'] - candle['low']) if (candle['high'] - candle['low']) > 0 else 0
                
                if body_ratio > 0.7:
                    if candle['close'] > candle['open'] and next_candle['close'] < candle['close']:
                        displacement_failed = True
                        break
                    elif candle['close'] < candle['open'] and next_candle['close'] > candle['close']:
                        displacement_failed = True
                        break
            
            if buy_side_hunted and sell_side_hunted and displacement_failed:
                return MarketState.ACCUMULATION
            
            # 2. Check for EXPANSION: Clean one-sided movement
            clean_expansion = False
            directional_bars = 0
            
            for i in range(-5, 0):
                if abs(i) >= len(df_1h):
                    continue
                
                candle = df_1h.iloc[i]
                body_ratio = abs(candle['close'] - candle['open']) / (candle['high'] - candle['low']) if (candle['high'] - candle['low']) > 0 else 0
                
                if body_ratio > 0.6:
                    if candle['close'] > candle['open']:
                        directional_bars += 1
                    else:
                        directional_bars -= 1
            
            if abs(directional_bars) >= 4 and (buy_side_hunted != sell_side_hunted):
                clean_expansion = True
            
            if clean_expansion:
                return MarketState.EXPANSION
            
            # 3. Check for DISTRIBUTION: Prior trend + failed continuation + opposite liquidity
            # Need to check higher timeframe for prior trend
            ohlcv_4h = await self._fetch_ohlcv(exchange, symbol, "4h", 20)
            if ohlcv_4h:
                df_4h = pd.DataFrame(ohlcv_4h, columns=["timestamp", "open", "high", "low", "close", "volume"])
                
                old_price = df_4h['close'].iloc[-10]
                new_price = df_4h['close'].iloc[-1]
                trend_exists = abs(new_price - old_price) / old_price > 0.03  # 3% move
                
                if trend_exists:
                    # Check for failed continuation on 1H
                    momentum_failed = False
                    early_momentum = self._calculate_momentum(df_1h.iloc[-10:-5])
                    recent_momentum = self._calculate_momentum(df_1h.iloc[-5:])
                    
                    if abs(recent_momentum) < abs(early_momentum) * 0.5:
                        momentum_failed = True
                    
                    # Check if opposite liquidity is resting
                    opposite_liquidity_resting = False
                    if liquidity_event['type'] in ['SWING_HIGH_SWEEP', 'RANGE_HIGH_SWEEP']:
                        # Bullish sweep occurred, check if bearish liquidity is below
                        swing_lows = self._find_swing_lows(df_1h, 3)
                        if swing_lows:
                            nearest_swing_low = max([l for l in swing_lows[-3:] if l < current_price], default=0)
                            if nearest_swing_low > 0 and current_price < nearest_swing_low * 1.02:
                                opposite_liquidity_resting = True
                    else:
                        # Bearish sweep occurred, check if bullish liquidity is above
                        swing_highs = self._find_swing_highs(df_1h, 3)
                        if swing_highs:
                            nearest_swing_high = min([h for h in swing_highs[-3:] if h > current_price], default=float('inf'))
                            if nearest_swing_high < float('inf') and current_price > nearest_swing_high * 0.98:
                                opposite_liquidity_resting = True
                    
                    if momentum_failed and opposite_liquidity_resting:
                        return MarketState.DISTRIBUTION
            
            return MarketState.UNCLEAR
            
        except Exception as e:
            self.log.debug(f"Market state intent error: {e}")
            return MarketState.UNCLEAR
    
    def _select_setup_by_state(self, market_state: MarketState, liquidity_event: Dict) -> Tuple[Optional[SetupType], str]:
        """State-dependent setup selection (RomeOTPT strict)"""
        
        event_type = liquidity_event['type']
        
        if market_state == MarketState.ACCUMULATION:
            if event_type in ['SWING_LOW_SWEEP', 'RANGE_LOW_SWEEP']:
                return SetupType.SWEEP_REVERSAL, "LONG"
            elif event_type in ['SWING_HIGH_SWEEP', 'RANGE_HIGH_SWEEP']:
                return SetupType.SWEEP_REVERSAL, "SHORT"
        
        elif market_state == MarketState.EXPANSION:
            # Direction determined by expansion bias (handled elsewhere)
            return SetupType.PULLBACK_CONTINUATION, ""  # Direction filled later
        
        elif market_state == MarketState.DISTRIBUTION:
            # RomeOTPT: Failed breakout (typically short, but depends)
            return SetupType.FAILED_BREAKOUT, "SHORT"
        
        return None, ""
    
    async def _analyze_entry_conditions(self, exchange, symbol: str, direction: str,
                                      liquidity_event: Dict, htf_narrative: Dict) -> Dict:
        """RomeOTPT Entry Logic (4 conditions)"""
        
        result = {
            'valid': False,
            'entry_tf': '',
            'entry_zone': (0.0, 0.0)
        }
        
        try:
            # Condition 1: Liquidity Taken (already confirmed)
            
            # Condition 2: Displacement
            displacement = await self._check_displacement(exchange, symbol, direction, liquidity_event['timeframe'])
            if not displacement['valid']:
                return result
            
            # Condition 3: Market Structure Shift
            entry_tf = self._get_entry_tf(liquidity_event['timeframe'])
            structure_shift = await self._check_structure_shift(exchange, symbol, direction, entry_tf)
            if not structure_shift['valid']:
                return result
            
            # Condition 4: Entry Location
            current_price = await self._get_price(exchange, symbol)
            
            if direction == "LONG":
                discount_zone = htf_narrative.get('discount_zone', (0.0, 0.0))
                if not (discount_zone[0] <= current_price <= discount_zone[1]):
                    return result
            else:  # SHORT
                premium_zone = htf_narrative.get('premium_zone', (0.0, 0.0))
                if not (premium_zone[0] <= current_price <= premium_zone[1]):
                    return result
            
            # Determine entry zone (RomeOTPT: anchor to displacement, not percentages)
            entry_zone = self._determine_entry_zone(direction, displacement['candle'])
            
            result.update({
                'valid': True,
                'entry_tf': entry_tf,
                'entry_zone': entry_zone
            })
            
        except Exception as e:
            self.log.debug(f"Entry conditions error: {e}")
        
        return result
    
    async def _check_displacement(self, exchange, symbol: str, direction: str, liquidity_tf: str) -> Dict:
        """Check for valid displacement candle"""
        result = {'valid': False, 'candle': {}}
        
        try:
            # Map liquidity TF to displacement TF
            tf_map = {"4h": "1h", "1h": "30m", "30m": "15m", "15m": "5m"}
            disp_tf = tf_map.get(liquidity_tf, "15m")
            
            ohlcv = await self._fetch_ohlcv(exchange, symbol, disp_tf, 10)
            if not ohlcv or len(ohlcv) < 3:
                return result
            
            df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
            
            # Look for displacement candle in last 3 candles
            for i in range(-3, 0):
                if abs(i) >= len(df):
                    continue
                
                candle = df.iloc[i]
                prev_candle = df.iloc[i-1] if i > -len(df) else None
                
                body_size = abs(candle['close'] - candle['open'])
                total_range = candle['high'] - candle['low']
                
                if total_range == 0:
                    continue
                
                body_ratio = body_size / total_range
                
                # RomeOTPT: Strong real body (>70%)
                if body_ratio >= 0.7:
                    is_bullish = candle['close'] > candle['open']
                    is_bearish = candle['close'] < candle['open']
                    
                    # Must match direction
                    if (direction == "LONG" and is_bullish) or (direction == "SHORT" and is_bearish):
                        # Must close through structure
                        closes_through = False
                        leaves_inefficiency = False
                        
                        if prev_candle is not None:
                            if direction == "LONG":
                                closes_through = candle['close'] > prev_candle['high']
                                leaves_inefficiency = candle['low'] > prev_candle['high']
                            else:
                                closes_through = candle['close'] < prev_candle['low']
                                leaves_inefficiency = candle['high'] < prev_candle['low']
                        
                        if closes_through and leaves_inefficiency:
                            result['valid'] = True
                            result['candle'] = {
                                'open': float(candle['open']),
                                'high': float(candle['high']),
                                'low': float(candle['low']),
                                'close': float(candle['close'])
                            }
                            break
            
        except Exception as e:
            self.log.debug(f"Displacement check error: {e}")
        
        return result
    
    async def _check_structure_shift(self, exchange, symbol: str, direction: str, entry_tf: str) -> Dict:
        """Check for market structure shift"""
        result = {'valid': False}
        
        try:
            ohlcv = await self._fetch_ohlcv(exchange, symbol, entry_tf, 30)
            if not ohlcv or len(ohlcv) < 10:
                return result
            
            df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
            current_price = df['close'].iloc[-1]
            
            # Find recent structure
            swing_highs = self._find_swing_highs(df, 2)
            swing_lows = self._find_swing_lows(df, 2)
            
            if direction == "LONG":
                # Need to break a lower high
                if len(swing_highs) >= 2:
                    recent_high = swing_highs[-1]
                    prev_high = swing_highs[-2] if len(swing_highs) >= 2 else 0
                    
                    if prev_high > recent_high:  # Lower high exists
                        result['valid'] = current_price > recent_high
            
            else:  # SHORT
                # Need to break a higher low
                if len(swing_lows) >= 2:
                    recent_low = swing_lows[-1]
                    prev_low = swing_lows[-2] if len(swing_lows) >= 2 else float('inf')
                    
                    if prev_low < recent_low:  # Higher low exists
                        result['valid'] = current_price < recent_low
            
        except Exception as e:
            self.log.debug(f"Structure shift check error: {e}")
        
        return result
    
    def _get_entry_tf(self, liquidity_tf: str) -> str:
        """Determine entry TF based on liquidity TF"""
        mapping = {
            "4h": "15m",
            "1h": "15m", 
            "30m": "5m",
            "15m": "5m"
        }
        return mapping.get(liquidity_tf, "15m")
    
    def _determine_entry_zone(self, direction: str, displacement_candle: Dict) -> Tuple[float, float]:
        """RomeOTPT: Anchor entry to displacement candle, not percentages"""
        
        if not displacement_candle:
            return (0.0, 0.0)
        
        candle_close = displacement_candle.get('close', 0)
        
        if direction == "LONG":
            # Enter near displacement candle close (within 0.2%)
            return (
                candle_close * 0.998,
                candle_close * 1.002
            )
        else:  # SHORT
            return (
                candle_close * 0.998,
                candle_close * 1.002
            )
    
    async def _find_tp_liquidity(self, exchange, symbol: str, direction: str, entry_tf: str) -> float:
        """Find untouched external liquidity for TP"""
        
        # TP must be on higher TF than entry
        higher_tf_map = {
            "5m": ["15m", "30m", "1h"],
            "15m": ["30m", "1h", "4h"],
            "30m": ["1h", "4h"],
            "1h": ["4h"]
        }
        
        target_tfs = higher_tf_map.get(entry_tf, ["1h", "4h"])
        
        try:
            for tf in target_tfs:
                ohlcv = await self._fetch_ohlcv(exchange, symbol, tf, 50)
                if not ohlcv:
                    continue
                
                df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
                current_price = df['close'].iloc[-1]
                
                if direction == "LONG":
                    swing_highs = self._find_swing_highs(df, 3)
                    for high in swing_highs[-5:]:
                        if high > current_price * 1.01:  # Must be above
                            recent_max = df['high'].iloc[-5:].max()
                            if high > recent_max:  # Untouched
                                return high
                else:
                    swing_lows = self._find_swing_lows(df, 3)
                    for low in swing_lows[-5:]:
                        if low < current_price * 0.99:  # Must be below
                            recent_min = df['low'].iloc[-5:].min()
                            if low < recent_min:  # Untouched
                                return low
        
        except Exception as e:
            self.log.debug(f"TP liquidity error: {e}")
        
        return 0.0
    
    def _is_signal_obvious(self, signal: Dict) -> bool:
        """RomeOTPT: Signal must be obvious to a discretionary trader"""
        
        # Check TP distance (not too far, not too close)
        current_price = signal.get('current_price', 0)
        tp = signal.get('take_profit_liquidity', 0)
        
        if current_price == 0 or tp == 0:
            return False
        
        distance_pct = abs(tp - current_price) / current_price * 100
        
        # RomeOTPT: TP should be 1-5% away for most assets
        if not (1.0 <= distance_pct <= 8.0):
            return False
        
        # Check entry zone width (should be tight)
        entry_zone = signal.get('entry_zone', (0.0, 0.0))
        zone_width_pct = (entry_zone[1] - entry_zone[0]) / entry_zone[0] * 100 if entry_zone[0] > 0 else 100
        
        if zone_width_pct > 0.5:  # Too wide
            return False
        
        # Check BTC context alignment
        btc_context = signal.get('btc_context', {})
        if btc_context.get('is_mid_range', False):
            return False
        
        return True
    
    async def _analyze_btc_directly(self, exchange) -> Optional[Dict]:
        """Special handling for BTC signals"""
        # BTC follows same logic but without BTC narrative filter
        # Implementation similar to regular assets
        return None
    
    # ============ HELPER METHODS ============
    
    async def _fetch_ohlcv(self, exchange, symbol: str, tf: str, limit: int):
        try:
            return await asyncio.wait_for(
                exchange.fetch_ohlcv(symbol, timeframe=tf, limit=limit),
                timeout=5.0
            )
        except:
            return None
    
    async def _get_price(self, exchange, symbol: str) -> float:
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
    
    def _calculate_sweep_strength(self, df: pd.DataFrame, level: float, side: str) -> float:
        """Calculate how strong a liquidity sweep was"""
        strength = 0.5
        
        # Find the candle that swept the level
        sweep_idx = -1
        for i in range(len(df)):
            if side == "HIGH" and df['high'].iloc[i] > level:
                sweep_idx = i
                break
            elif side == "LOW" and df['low'].iloc[i] < level:
                sweep_idx = i
                break
        
        if sweep_idx == -1 or sweep_idx >= len(df) - 1:
            return strength
        
        sweep_candle = df.iloc[sweep_idx]
        next_candle = df.iloc[sweep_idx + 1]
        
        # Wick size relative to body
        if side == "HIGH":
            wick = sweep_candle['high'] - max(sweep_candle['open'], sweep_candle['close'])
            body = abs(sweep_candle['close'] - sweep_candle['open'])
            if body > 0 and wick / body > 1.0:
                strength += 0.2
            
            # Rejection strength
            if next_candle['close'] < sweep_candle['close']:
                strength += 0.3
        else:
            wick = min(sweep_candle['open'], sweep_candle['close']) - sweep_candle['low']
            body = abs(sweep_candle['close'] - sweep_candle['open'])
            if body > 0 and wick / body > 1.0:
                strength += 0.2
            
            if next_candle['close'] > sweep_candle['close']:
                strength += 0.3
        
        return min(strength, 1.0)
    
    def _calculate_momentum(self, df: pd.DataFrame) -> float:
        if len(df) < 2:
            return 0.0
        return (df['close'].iloc[-1] - df['close'].iloc[0]) / df['close'].iloc[0]

# ---------------- SIGNAL OUTPUT ----------------
def format_romeopt_signal(signal: Dict) -> str:
    """Output in exact RomeOTPT format"""
    
    if not signal:
        return "NO TRADE — CONDITIONS NOT MET"
    
    return f"""ASSET: {signal.get('asset', 'UNKNOWN')}
DIRECTION: {signal.get('direction', 'UNKNOWN')}
MARKET STATE: {signal.get('market_state', 'UNKNOWN')}
SETUP TYPE: {signal.get('setup_type', 'UNKNOWN')}
HTF NARRATIVE: {json.dumps(signal.get('htf_narrative', {}), indent=2)}
LIQUIDITY TAKEN: {json.dumps(signal.get('liquidity_taken', {}), indent=2)}
ENTRY ZONE: {signal.get('entry_zone', (0, 0))[0]:.8f} - {signal.get('entry_zone', (0, 0))[1]:.8f}
ENTRY TF: {signal.get('entry_tf', 'UNKNOWN')}
TAKE PROFIT LIQUIDITY: {signal.get('take_profit_liquidity', 0):.8f}
REASON (CAUSE → EFFECT): {signal.get('reason', 'No reason')}"""

async def send_romeopt_alert(signal: Dict):
    """Send formatted alert"""
    try:
        formatted = format_romeopt_signal(signal)
        
        # Telegram formatting
        emoji = "🟢" if signal.get('direction') == "LONG" else "🔴"
        state = signal.get('market_state', 'UNKNOWN')
        state_emoji = {
            "ACCUMULATION": "🟡",
            "EXPANSION": "🟢", 
            "DISTRIBUTION": "🔴"
        }.get(state, "⚪")
        
        msg = f"""
{emoji}{state_emoji} <b>ROMEOTPT v5.0 - INSTITUTIONAL SIGNAL</b>

<b>🎯 {signal.get('asset', 'UNKNOWN')}</b> | {signal.get('direction', 'UNKNOWN')}
<b>State:</b> {state}
<b>Setup:</b> {signal.get('setup_type', 'UNKNOWN')}

<b>📍 Entry:</b> {signal.get('entry_zone', (0, 0))[0]:.8f} - {signal.get('entry_zone', (0, 0))[1]:.8f}
<b>🎯 TP Liquidity:</b> {signal.get('take_profit_liquidity', 0):.8f}
<b>📊 TF:</b> {signal.get('entry_tf', 'UNKNOWN')}

<b>🧠 Cause → Effect:</b>
{signal.get('reason', 'No reason')}

<b>₿ BTC Context:</b>
{json.dumps(signal.get('btc_context', {}), indent=2)}

<i>Detected: {datetime.datetime.utcnow().strftime('%H:%M:%S UTC')}</i>
"""
        
        await send_telegram(msg)
        await send_telegram(f"<code>{formatted}</code>")
        
    except Exception as e:
        log.error(f"Alert error: {e}")

async def send_telegram(msg: str, parse_mode="HTML"):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            await client.post(url, json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": msg,
                "parse_mode": parse_mode
            })
        except:
            pass

# ---------------- MAIN SCANNER ----------------
async def main_scanner():
    """RomeOTPT Institutional Scanner"""
    
    exchange = ccxt.okx({
        "enableRateLimit": True,
        "options": {"defaultType": "spot"},
        "rateLimit": 10,
        "timeout": 5000,
    })
    
    # Initialize BTC narrative engine
    btc_engine = BTCNarrativeEngine()
    
    # Signal tracker
    active_signals = {}
    last_btc_update = datetime.datetime.utcnow() - datetime.timedelta(minutes=10)
    btc_narrative = None
    
    startup_msg = f"""
🚀 <b>ROMEOTPT v5.0 - INSTITUTIONAL SCANNER</b>
<i>Pure Trader Logic, No Engineer Thinking</i>

<b>Core Fixes Applied:</b>
• BTC Narrative Filter (Primary)
• Behavior-Based HTF Analysis  
• Binary Intent State Detection
• Single Strongest Liquidity Only
• Displacement-Anchor Entry Zones

<b>Settings:</b>
• Scan: {SCAN_INTERVAL}s
• Top: {TOP_N} symbols
• Cooldown: {SIGNAL_COOLDOWN_MINUTES}min
"""
    await send_telegram(startup_msg)
    
    scan_cycle = 0
    
    while True:
        scan_cycle += 1
        
        try:
            # Update BTC narrative every 5 minutes
            current_time = datetime.datetime.utcnow()
            if (current_time - last_btc_update).total_seconds() > 300 or btc_narrative is None:
                btc_narrative = await btc_engine.analyze(exchange)
                last_btc_update = current_time
                log.info(f"₿ BTC Narrative: {btc_narrative.bias} {btc_narrative.market_state.value}")
            
            # Get top symbols
            tickers = await exchange.fetch_tickers()
            usdt_pairs = []
            
            for symbol, data in tickers.items():
                if symbol.endswith("/USDT") and "USDC" not in symbol:
                    volume = data.get("quoteVolume", 0)
                    if isinstance(volume, (int, float)) and volume > 1000000:  # 1M+ volume
                        usdt_pairs.append((symbol, float(volume)))
            
            usdt_pairs.sort(key=lambda x: x[1], reverse=True)
            symbols_to_scan = [s[0] for s in usdt_pairs[:TOP_N]]
            
            log.info(f"🔄 Scan #{scan_cycle}: {len(symbols_to_scan)} symbols | BTC: {btc_narrative.bias}")
            
            # Initialize trader engine with BTC context
            trader_engine = ROMEOTPTTraderEngine(btc_narrative)
            
            # Scan symbols
            signals_found = 0
            tasks = []
            
            for symbol in symbols_to_scan:
                task = asyncio.create_task(trader_engine.analyze_asset(exchange, symbol))
                tasks.append(task)
                
                if len(tasks) >= MAX_CONCURRENT:
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    for result in results:
                        if isinstance(result, Exception):
                            continue
                        
                        if result:
                            symbol = result.get('asset')
                            
                            # Check cooldown
                            if symbol in active_signals:
                                last_alert = active_signals[symbol].get('last_alert')
                                if last_alert:
                                    minutes_since = (current_time - last_alert).total_seconds() / 60
                                    if minutes_since < SIGNAL_COOLDOWN_MINUTES:
                                        continue
                            
                            # Send alert
                            await send_romeopt_alert(result)
                            signals_found += 1
                            
                            # Update tracker
                            active_signals[symbol] = {
                                'last_alert': current_time,
                                'signal': result
                            }
                    
                    tasks = []
            
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for result in results:
                    if isinstance(result, Exception):
                        continue
                    
                    if result:
                        symbol = result.get('asset')
                        
                        if symbol in active_signals:
                            last_alert = active_signals[symbol].get('last_alert')
                            if last_alert:
                                minutes_since = (current_time - last_alert).total_seconds() / 60
                                if minutes_since < SIGNAL_COOLDOWN_MINUTES:
                                    continue
                        
                        await send_romeopt_alert(result)
                        signals_found += 1
                        active_signals[symbol] = {
                            'last_alert': current_time,
                            'signal': result
                        }
            
            log.info(f"📊 Scan #{scan_cycle} complete: {signals_found} signals")
            
            # Cleanup old signals
            expired = []
            for symbol, data in active_signals.items():
                age_hours = (current_time - data['last_alert']).total_seconds() / 3600
                if age_hours > SIGNAL_VALIDITY_HOURS:
                    expired.append(symbol)
            
            for symbol in expired:
                del active_signals[symbol]
            
            if expired:
                log.debug(f"🧹 Cleaned {len(expired)} expired signals")
            
            await asyncio.sleep(SCAN_INTERVAL)
            
        except Exception as e:
            log.error(f"Scanner error: {e}")
            await asyncio.sleep(SCAN_INTERVAL * 2)

# ---------------- MAIN ----------------
if __name__ == "__main__":
    log.info("🚀 ROMEOTPT v5.0 - INSTITUTIONAL THINKING")
    log.info("Core: BTC Narrative → Behavior Analysis → Binary Intent")
    
    try:
        asyncio.run(main_scanner())
    except KeyboardInterrupt:
        log.info("Scanner stopped by user")