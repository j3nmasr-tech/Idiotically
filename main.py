#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🧠 COMPLETE TRADER SYSTEM - ULTRA ROBUST VERSION
Professional Discretionary Trading Engine
7-Tool Direction Analysis + 3 Price-Only Entry Types
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
from dataclasses import dataclass
import json

# ================ CONFIGURATION ================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DB_PATH = "/app/data/trader_system.db"

# Exchange configuration
EXCHANGE = "okx"
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 10))
TOP_N_VOLUME = int(os.getenv("TOP_N_VOLUME", 50))
MIN_VOLUME_USD = 1000000

# Risk Management
MAX_POSITIONS = 5
MAX_STOP_LOSS_PCT = 1.5
MIN_TARGET_PCT = 2.0
MIN_RISK_REWARD = 2.0

# Timeframes
TIMEFRAMES = {
    "1H": "1h",
    "15M": "15m",
    "5M": "5m",
    "3M": "3m",
    "1M": "1m"
}

# ================ LOGGING ================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)8s | %(name)s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger("trader_system")

# ================ ULTRA SAFE DATA HANDLING ================
def is_valid_df(df) -> bool:
    """ULTRA SAFE DataFrame validation - NO truth value checks EVER"""
    if df is None:
        return False
    
    # Check type first
    if not isinstance(df, pd.DataFrame):
        return False
    
    # Check if empty
    try:
        if df.empty:
            return False
    except Exception:
        return False
    
    # Check length
    try:
        if len(df) < 20:
            return False
    except Exception:
        return False
    
    # Check required columns
    required = ['open', 'high', 'low', 'close', 'volume']
    try:
        for col in required:
            if col not in df.columns:
                return False
    except Exception:
        return False
    
    # Check for NaN
    try:
        for col in required:
            series = df[col]
            if series.isnull().any():
                return False
    except Exception:
        return False
    
    return True

def safe_get(df, column, index=-1, default=0):
    """Safely get value from DataFrame"""
    try:
        if is_valid_df(df):
            return df[column].iloc[index]
        return default
    except Exception:
        return default

# ================ TECHNICAL INDICATORS ================
class TechnicalIndicators:
    
    @staticmethod
    def EMA(prices: pd.Series, period: int) -> pd.Series:
        return prices.ewm(span=period, adjust=False).mean()
    
    @staticmethod
    def RSI(prices: pd.Series, period: int = 14) -> pd.Series:
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    @staticmethod
    def ATR(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        return tr.rolling(window=period).mean()

# ================ DATA STRUCTURES ================
@dataclass
class DirectionAnalysis:
    direction: str
    tool_results: Dict[str, str]
    confidence: float

@dataclass  
class MarketState:
    state: str
    volatility: float
    trend_strength: float
    compression: bool

@dataclass
class PriceEntry:
    entry_type: str
    entry_price: float
    stop_loss: float
    take_profit: float
    details: Dict[str, Any]
    timestamp: float

@dataclass
class TradeSignal:
    signal_id: str
    symbol: str
    direction: str
    entry: PriceEntry
    market_state: MarketState
    direction_analysis: DirectionAnalysis
    risk_reward: float
    conditions_met: List[str]

# ================ TOOL 1: MULTI-TF AGREEMENT ================
def analyze_multi_timeframe_agreement(multi_tf_data: Dict[str, pd.DataFrame]) -> str:
    directions = []
    
    for tf_name, df in multi_tf_data.items():
        if not is_valid_df(df):
            continue
            
        try:
            ema20 = TechnicalIndicators.EMA(df['close'], 20).iloc[-1]
            ema50 = TechnicalIndicators.EMA(df['close'], 50).iloc[-1]
            rsi = TechnicalIndicators.RSI(df['close'], 14).iloc[-1]
            
            if np.isnan(ema20) or np.isnan(ema50) or np.isnan(rsi):
                continue
                
            if ema20 > ema50 and rsi > 50:
                direction = "LONG"
            elif ema20 < ema50 and rsi < 50:
                direction = "SHORT"
            else:
                direction = "NEUTRAL"
            
            directions.append((tf_name, direction))
            
        except Exception:
            continue
    
    if not directions:
        return "NEUTRAL"
    
    major_tfs = ["1H", "15M", "5M"]
    major_directions = [d for tf, d in directions if tf in major_tfs]
    
    if len(major_directions) >= 2:
        if all(d == "LONG" for d in major_directions):
            return "LONG"
        elif all(d == "SHORT" for d in major_directions):
            return "SHORT"
    
    return "NEUTRAL"

# ================ TOOL 2: WAVE LENGTH ================
def analyze_wave_length(df: pd.DataFrame) -> bool:
    try:
        if not is_valid_df(df):
            return False
        
        if len(df) < 20:
            return False
            
        recent = df.iloc[-10:]
        
        impulse_start = recent['low'].iloc[-5]
        impulse_end = recent['high'].iloc[-1]
        impulse_move = impulse_end - impulse_start
        
        if impulse_move <= 0:
            return False
        
        pullback_start = recent['high'].iloc[-10]
        pullback_end = recent['low'].iloc[-5]
        pullback_move = abs(pullback_end - pullback_start)
        
        if pullback_move > 0:
            wave_ratio = impulse_move / pullback_move
            return wave_ratio > 1.5
        else:
            return impulse_move > 0
        
    except Exception:
        return False

# ================ TOOL 3: MOMENTUM STRENGTH ================
def analyze_momentum_strength(df: pd.DataFrame) -> bool:
    try:
        if not is_valid_df(df):
            return False
        
        current = df.iloc[-1]
        body = abs(current['close'] - current['open'])
        
        atr = TechnicalIndicators.ATR(df['high'], df['low'], df['close'], 14).iloc[-1]
        
        if atr <= 0 or np.isnan(atr):
            return False
        
        rsi = TechnicalIndicators.RSI(df['close'], 14).iloc[-1]
        
        if np.isnan(rsi):
            return False
        
        body_strength = body / atr
        return body_strength > 0.7 and rsi > 55
        
    except Exception:
        return False

# ================ TOOL 4: VOLUME PARTICIPATION ================
def analyze_volume_participation(df: pd.DataFrame) -> bool:
    try:
        if not is_valid_df(df):
            return False
        
        recent_volume = df['volume'].iloc[-1]
        avg_volume = df['volume'].iloc[-20:].mean()
        
        if avg_volume > 0:
            volume_ratio = recent_volume / avg_volume
            return volume_ratio > 1.2
        else:
            return recent_volume > 0
        
    except Exception:
        return False

# ================ TOOL 5: RSI REGIME ================
def analyze_rsi_regime(df: pd.DataFrame, target_direction: str) -> bool:
    try:
        if not is_valid_df(df):
            return False
        
        rsi = TechnicalIndicators.RSI(df['close'], 14).iloc[-1]
        
        if np.isnan(rsi):
            return False
        
        if target_direction == "LONG":
            return rsi > 50
        elif target_direction == "SHORT":
            return rsi < 50
        else:
            return False
            
    except Exception:
        return False

# ================ TOOL 6: EMA STRUCTURE ================
def analyze_ema_structure(df: pd.DataFrame, target_direction: str) -> bool:
    try:
        if not is_valid_df(df):
            return False
        
        ema20 = TechnicalIndicators.EMA(df['close'], 20).iloc[-1]
        ema50 = TechnicalIndicators.EMA(df['close'], 50).iloc[-1]
        
        if np.isnan(ema20) or np.isnan(ema50):
            return False
        
        if target_direction == "LONG":
            return ema20 > ema50
        elif target_direction == "SHORT":
            return ema20 < ema50
        else:
            return False
            
    except Exception:
        return False

# ================ TOOL 7: VOLATILITY TRADABILITY ================
def analyze_volatility_tradability(df: pd.DataFrame) -> bool:
    try:
        if not is_valid_df(df):
            return False
        
        atr = TechnicalIndicators.ATR(df['high'], df['low'], df['close'], 14)
        current_atr = atr.iloc[-1]
        avg_atr = atr.iloc[-20:].mean()
        
        if np.isnan(current_atr) or np.isnan(avg_atr) or avg_atr == 0:
            return False
        
        return current_atr > avg_atr * 0.7
            
    except Exception:
        return False

# ================ DIRECTION ENGINE ================
class DirectionEngine:
    
    def __init__(self):
        self.analysis_count = 0
        self.direction_stats = {
            "LONG": 0,
            "SHORT": 0,
            "NO_TRADE": 0,
            "tool_failures": {}
        }
    
    def analyze_direction(self, multi_tf_data: Dict[str, pd.DataFrame]) -> DirectionAnalysis:
        self.analysis_count += 1
        
        log.info(f"🧠 Direction Analysis #{self.analysis_count}")
        
        # Primary timeframe - NO "if df" checks!
        primary_df = None
        if "15M" in multi_tf_data:
            primary_df = multi_tf_data["15M"]
        elif "5M" in multi_tf_data:
            primary_df = multi_tf_data["5M"]
        
        if not is_valid_df(primary_df):
            log.warning("❌ No primary timeframe data")
            return DirectionAnalysis("NO_TRADE", {}, 0.0)
        
        # 1️⃣ Multi-Timeframe Agreement
        mtf_direction = analyze_multi_timeframe_agreement(multi_tf_data)
        
        if mtf_direction == "NEUTRAL":
            log.warning("❌ MTF Disagreement → NO TRADE")
            self.direction_stats["NO_TRADE"] += 1
            return DirectionAnalysis("NO_TRADE", {"MTF": "NEUTRAL"}, 0.0)
        
        log.info(f"✅ MTF Agreement: {mtf_direction}")
        
        tool_results = {}
        tool_failures = []
        
        # 2️⃣ Wave Length
        wave_ok = analyze_wave_length(primary_df)
        tool_results["Wave_Length"] = "PASS" if wave_ok else "FAIL"
        if not wave_ok:
            tool_failures.append("Wave_Length")
        
        # 3️⃣ Momentum Strength
        strength_ok = analyze_momentum_strength(primary_df)
        tool_results["Momentum_Strength"] = "PASS" if strength_ok else "FAIL"
        if not strength_ok:
            tool_failures.append("Momentum_Strength")
        
        # 4️⃣ Volume Participation
        volume_ok = analyze_volume_participation(primary_df)
        tool_results["Volume_Participation"] = "PASS" if volume_ok else "FAIL"
        if not volume_ok:
            tool_failures.append("Volume_Participation")
        
        # 5️⃣ RSI Regime
        rsi_ok = analyze_rsi_regime(primary_df, mtf_direction)
        tool_results["RSI_Regime"] = "PASS" if rsi_ok else "FAIL"
        if not rsi_ok:
            tool_failures.append("RSI_Regime")
        
        # 6️⃣ EMA Structure
        ema_ok = analyze_ema_structure(primary_df, mtf_direction)
        tool_results["EMA_Structure"] = "PASS" if ema_ok else "FAIL"
        if not ema_ok:
            tool_failures.append("EMA_Structure")
        
        # 7️⃣ Volatility Tradability
        vol_ok = analyze_volatility_tradability(primary_df)
        tool_results["Volatility"] = "PASS" if vol_ok else "FAIL"
        if not vol_ok:
            tool_failures.append("Volatility")
        
        # Check if ALL tools passed
        all_passed = len(tool_failures) == 0
        
        if all_passed:
            confidence = 0.9
            
            log.info(f"🎯 **ALL 7 TOOLS AGREE** → {mtf_direction} DIRECTION LOCKED")
            
            self.direction_stats[mtf_direction] += 1
            
            return DirectionAnalysis(
                direction=mtf_direction,
                tool_results=tool_results,
                confidence=confidence
            )
        else:
            for tool in tool_failures:
                self.direction_stats["tool_failures"][tool] = \
                    self.direction_stats["tool_failures"].get(tool, 0) + 1
            
            log.warning(f"❌ {len(tool_failures)} tools failed → NO TRADE")
            self.direction_stats["NO_TRADE"] += 1
            
            return DirectionAnalysis(
                direction="NO_TRADE",
                tool_results=tool_results,
                confidence=0.0
            )
    
    def get_stats(self) -> Dict:
        return self.direction_stats

# ================ MARKET STATE DETECTION ================
def detect_market_state(df: pd.DataFrame) -> MarketState:
    try:
        if not is_valid_df(df):
            return MarketState("FAST_MARKET", 0.0, 0.0, False)
        
        atr = TechnicalIndicators.ATR(df['high'], df['low'], df['close'], 14)
        current_atr = atr.iloc[-1]
        avg_atr = atr.iloc[-20:].mean()
        
        if np.isnan(current_atr) or np.isnan(avg_atr) or avg_atr == 0:
            volatility_ratio = 1.0
        else:
            volatility_ratio = current_atr / avg_atr
        
        ema20 = TechnicalIndicators.EMA(df['close'], 20)
        if len(ema20) >= 10:
            ema_slope = (ema20.iloc[-1] - ema20.iloc[-10]) / ema20.iloc[-10] * 100
        else:
            ema_slope = 0.0
        
        # Compression
        recent_high = df['high'].iloc[-5:].max()
        recent_low = df['low'].iloc[-5:].min()
        recent_range = recent_high - recent_low
        
        avg_candle_size = abs(df['close'].iloc[-5:] - df['open'].iloc[-5:]).mean()
        
        if avg_candle_size > 0:
            compression_ratio = recent_range / avg_candle_size
            compression = compression_ratio < 3
        else:
            compression = False
        
        # Determine state
        if abs(ema_slope) > 1.5 and volatility_ratio > 1.2:
            state = "STRONG_TREND"
        elif compression and df['volume'].iloc[-1] < df['volume'].iloc[-20:].mean() * 0.8:
            state = "FAST_MARKET"
        elif volatility_ratio > 1.5:
            state = "VOLATILE_SPIKES"
        else:
            state = "FAST_MARKET"
        
        return MarketState(
            state=state,
            volatility=volatility_ratio,
            trend_strength=abs(ema_slope),
            compression=compression
        )
        
    except Exception as e:
        log.error(f"Market state error: {e}")
        return MarketState("FAST_MARKET", 1.0, 0.0, False)

# ================ PRICE ENTRY ENGINE ================
class PriceEntryEngine:
    
    def __init__(self):
        self.entry_stats = {
            "pullback_entries": 0,
            "breakout_entries": 0,
            "stophunt_entries": 0,
            "no_entry": 0
        }
    
    def check_pullback_entry(self, df: pd.DataFrame, direction: str) -> Tuple[bool, Dict]:
        try:
            if not is_valid_df(df):
                return False, {}
            
            current = df.iloc[-1]
            prev = df.iloc[-2]
            
            ema20 = TechnicalIndicators.EMA(df['close'], 20).iloc[-1]
            
            if np.isnan(ema20):
                return False, {}
            
            volume_decreasing = df['volume'].iloc[-1] < df['volume'].iloc[-3]
            
            if direction == "LONG":
                price_above_ema = current['close'] > ema20
                
                weak_previous = False
                for i in range(-4, -1):
                    if i < 0:
                        candle = df.iloc[i]
                        body = abs(candle['close'] - candle['open'])
                        high_low = candle['high'] - candle['low']
                        if high_low > 0:
                            body_ratio = body / high_low
                            if body_ratio < 0.4:
                                weak_previous = True
                                break
                
                strong_bullish = (
                    current['close'] > current['open'] and
                    (current['close'] - current['open']) > (prev['high'] - prev['low']) * 0.4
                )
                
                entry_valid = (
                    price_above_ema and
                    volume_decreasing and
                    weak_previous and
                    strong_bullish
                )
                
                details = {
                    "ema20": ema20,
                    "price_above_ema": price_above_ema,
                    "volume_decreasing": volume_decreasing,
                    "weak_previous_candles": weak_previous,
                    "strong_bullish_candle": strong_bullish,
                    "entry_price": current['close']
                }
                
                return entry_valid, details
                
            elif direction == "SHORT":
                price_below_ema = current['close'] < ema20
                
                weak_previous = False
                for i in range(-4, -1):
                    if i < 0:
                        candle = df.iloc[i]
                        body = abs(candle['close'] - candle['open'])
                        high_low = candle['high'] - candle['low']
                        if high_low > 0:
                            body_ratio = body / high_low
                            if body_ratio < 0.4:
                                weak_previous = True
                                break
                
                strong_bearish = (
                    current['close'] < current['open'] and
                    (current['open'] - current['close']) > (prev['high'] - prev['low']) * 0.4
                )
                
                entry_valid = (
                    price_below_ema and
                    volume_decreasing and
                    weak_previous and
                    strong_bearish
                )
                
                details = {
                    "ema20": ema20,
                    "price_below_ema": price_below_ema,
                    "volume_decreasing": volume_decreasing,
                    "weak_previous_candles": weak_previous,
                    "strong_bearish_candle": strong_bearish,
                    "entry_price": current['close']
                }
                
                return entry_valid, details
            else:
                return False, {}
                
        except Exception:
            return False, {}
    
    def check_breakout_entry(self, df: pd.DataFrame, direction: str) -> Tuple[bool, Dict]:
        try:
            if not is_valid_df(df):
                return False, {}
            
            current = df.iloc[-1]
            prev = df.iloc[-2]
            
            compression_candles = df.iloc[-5:-1]
            if len(compression_candles) < 3:
                return False, {}
            
            ranges = compression_candles['high'] - compression_candles['low']
            avg_range = ranges.mean()
            current_range = current['high'] - current['low']
            
            range_compression = current_range < avg_range * 0.7 if avg_range > 0 else False
            
            compression_volume = compression_candles['volume'].mean()
            volume_drop = compression_volume < df['volume'].iloc[-10:-5].mean() * 0.8
            
            if direction == "LONG":
                break_high = current['close'] > prev['high']
                volume_spike = current['volume'] > compression_volume * 1.5
                strong_candle = current['close'] > current['open'] and \
                               (current['close'] - current['open']) > current_range * 0.6
                
                entry_valid = (
                    range_compression and
                    volume_drop and
                    break_high and
                    volume_spike and
                    strong_candle
                )
                
                details = {
                    "range_compression": range_compression,
                    "volume_drop": volume_drop,
                    "break_high": break_high,
                    "volume_spike": volume_spike,
                    "strong_candle": strong_candle,
                    "breakout_level": prev['high'],
                    "entry_price": current['close']
                }
                
                return entry_valid, details
                
            elif direction == "SHORT":
                break_low = current['close'] < prev['low']
                volume_spike = current['volume'] > compression_volume * 1.5
                strong_candle = current['close'] < current['open'] and \
                               (current['open'] - current['close']) > current_range * 0.6
                
                entry_valid = (
                    range_compression and
                    volume_drop and
                    break_low and
                    volume_spike and
                    strong_candle
                )
                
                details = {
                    "range_compression": range_compression,
                    "volume_drop": volume_drop,
                    "break_low": break_low,
                    "volume_spike": volume_spike,
                    "strong_candle": strong_candle,
                    "breakout_level": prev['low'],
                    "entry_price": current['close']
                }
                
                return entry_valid, details
            else:
                return False, {}
                
        except Exception:
            return False, {}
    
    def check_stophunt_entry(self, df: pd.DataFrame, direction: str) -> Tuple[bool, Dict]:
        try:
            if not is_valid_df(df):
                return False, {}
            
            current = df.iloc[-1]
            prev = df.iloc[-2]
            
            if direction == "LONG":
                bearish_wick = prev['low'] < min(prev['open'], prev['close'])
                if not bearish_wick:
                    return False, {}
                
                wick_size = min(prev['open'], prev['close']) - prev['low']
                body_size = abs(prev['close'] - prev['open'])
                
                if body_size == 0:
                    return False, {}
                
                significant_wick = wick_size > body_size * 1.5
                immediate_recovery = current['close'] > prev['close']
                volume_climax = prev['volume'] > df['volume'].iloc[-5:-1].mean() * 1.5
                ema20 = TechnicalIndicators.EMA(df['close'], 20).iloc[-1]
                reclaim = current['close'] > ema20
                
                entry_valid = (
                    significant_wick and
                    immediate_recovery and
                    volume_climax and
                    reclaim
                )
                
                details = {
                    "wick_size": wick_size,
                    "body_size": body_size,
                    "wick_ratio": wick_size / body_size,
                    "immediate_recovery": immediate_recovery,
                    "volume_climax": volume_climax,
                    "reclaim_ema20": reclaim,
                    "ema20": ema20,
                    "entry_price": current['close']
                }
                
                return entry_valid, details
                
            elif direction == "SHORT":
                bullish_wick = prev['high'] > max(prev['open'], prev['close'])
                if not bullish_wick:
                    return False, {}
                
                wick_size = prev['high'] - max(prev['open'], prev['close'])
                body_size = abs(prev['close'] - prev['open'])
                
                if body_size == 0:
                    return False, {}
                
                significant_wick = wick_size > body_size * 1.5
                immediate_recovery = current['close'] < prev['close']
                volume_climax = prev['volume'] > df['volume'].iloc[-5:-1].mean() * 1.5
                ema20 = TechnicalIndicators.EMA(df['close'], 20).iloc[-1]
                reclaim = current['close'] < ema20
                
                entry_valid = (
                    significant_wick and
                    immediate_recovery and
                    volume_climax and
                    reclaim
                )
                
                details = {
                    "wick_size": wick_size,
                    "body_size": body_size,
                    "wick_ratio": wick_size / body_size,
                    "immediate_recovery": immediate_recovery,
                    "volume_climax": volume_climax,
                    "reclaim_ema20": reclaim,
                    "ema20": ema20,
                    "entry_price": current['close']
                }
                
                return entry_valid, details
            else:
                return False, {}
                
        except Exception:
            return False, {}
    
    def find_entry(self, df: pd.DataFrame, direction: str, market_state: MarketState) -> Optional[PriceEntry]:
        try:
            if not is_valid_df(df):
                return None
                
            log.info(f"🎯 Looking for {direction} entries (Market: {market_state.state})...")
            
            entry_found = False
            entry_type = None
            entry_details = {}
            
            # Prioritize entry types based on market state
            if market_state.state == "STRONG_TREND":
                entry_found, entry_details = self.check_pullback_entry(df, direction)
                if entry_found:
                    entry_type = "PULLBACK"
                    self.entry_stats["pullback_entries"] += 1
                else:
                    entry_found, entry_details = self.check_breakout_entry(df, direction)
                    if entry_found:
                        entry_type = "BREAKOUT"
                        self.entry_stats["breakout_entries"] += 1
            
            elif market_state.state == "FAST_MARKET":
                entry_found, entry_details = self.check_breakout_entry(df, direction)
                if entry_found:
                    entry_type = "BREAKOUT"
                    self.entry_stats["breakout_entries"] += 1
                else:
                    entry_found, entry_details = self.check_pullback_entry(df, direction)
                    if entry_found:
                        entry_type = "PULLBACK"
                        self.entry_stats["pullback_entries"] += 1
            
            elif market_state.state == "VOLATILE_SPIKES":
                entry_found, entry_details = self.check_stophunt_entry(df, direction)
                if entry_found:
                    entry_type = "STOPHUNT"
                    self.entry_stats["stophunt_entries"] += 1
                else:
                    entry_found, entry_details = self.check_breakout_entry(df, direction)
                    if entry_found:
                        entry_type = "BREAKOUT"
                        self.entry_stats["breakout_entries"] += 1
            
            # If still no entry, try all types
            if not entry_found:
                for check_func, etype in [
                    (self.check_pullback_entry, "PULLBACK"),
                    (self.check_breakout_entry, "BREAKOUT"),
                    (self.check_stophunt_entry, "STOPHUNT")
                ]:
                    entry_found, entry_details = check_func(df, direction)
                    if entry_found:
                        entry_type = etype
                        self.entry_stats[f"{etype.lower()}_entries"] += 1
                        break
            
            if not entry_found:
                self.entry_stats["no_entry"] += 1
                log.info("❌ No price entry trigger found")
                return None
            
            # Calculate stop loss and take profit
            entry_price = entry_details.get("entry_price", df['close'].iloc[-1])
            
            # Dynamic stop loss based on ATR
            atr = TechnicalIndicators.ATR(df['high'], df['low'], df['close'], 14).iloc[-1]
            stop_distance = min(atr * 1.5, entry_price * MAX_STOP_LOSS_PCT / 100)
            
            if direction == "LONG":
                stop_loss = entry_price - stop_distance
                take_profit = entry_price + (stop_distance * MIN_RISK_REWARD)
            else:
                stop_loss = entry_price + stop_distance
                take_profit = entry_price - (stop_distance * MIN_RISK_REWARD)
            
            # Ensure minimum target
            min_target_distance = entry_price * MIN_TARGET_PCT / 100
            if direction == "LONG":
                take_profit = max(take_profit, entry_price + min_target_distance)
            else:
                take_profit = min(take_profit, entry_price - min_target_distance)
            
            # Calculate risk/reward
            risk = abs(entry_price - stop_loss)
            reward = abs(take_profit - entry_price)
            risk_reward = reward / risk if risk > 0 else 0
            
            if risk_reward < MIN_RISK_REWARD:
                log.warning(f"❌ Risk/Reward too low: {risk_reward:.1f}:1")
                return None
            
            log.info(f"✅ {entry_type} ENTRY FOUND for {direction}")
            log.info(f"   Entry: {entry_price:.4f}, SL: {stop_loss:.4f}, TP: {take_profit:.4f}")
            log.info(f"   R:R: {risk_reward:.1f}:1")
            
            return PriceEntry(
                entry_type=entry_type,
                entry_price=entry_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
                details=entry_details,
                timestamp=time.time()
            )
            
        except Exception as e:
            log.error(f"Find entry error: {e}")
            return None
    
    def get_stats(self) -> Dict:
        return self.entry_stats

# ================ COMPLETE TRADER SYSTEM ================
class CompleteTraderSystem:
    
    def __init__(self):
        self.direction_engine = DirectionEngine()
        self.price_entry_engine = PriceEntryEngine()
        self.exchange = None
        self.db = None
        self.active_trades = {}
    
    async def initialize(self):
        log.info("=" * 70)
        log.info("🧠 COMPLETE TRADER SYSTEM - ULTRA ROBUST")
        log.info("=" * 70)
        
        await self._init_database()
        await self._init_exchange()
        await self._send_startup_message()
    
    async def _init_database(self):
        try:
            os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
            self.db = await aiosqlite.connect(DB_PATH)
            
            await self.db.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL,
                entry_type TEXT NOT NULL,
                entry_price REAL NOT NULL,
                stop_loss REAL NOT NULL,
                take_profit REAL NOT NULL,
                market_state TEXT NOT NULL,
                volatility REAL NOT NULL,
                trend_strength REAL NOT NULL,
                tool_results TEXT NOT NULL,
                confidence REAL NOT NULL,
                risk_reward REAL NOT NULL,
                status TEXT DEFAULT 'PENDING',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                triggered_at TIMESTAMP,
                trigger_price REAL,
                closed_at TIMESTAMP,
                close_price REAL,
                pnl_percent REAL,
                close_reason TEXT,
                conditions_met TEXT
            )
            """)
            
            await self.db.commit()
            log.info("✅ Database initialized")
            
        except Exception as e:
            log.error(f"Database error: {e}")
            raise
    
    async def _init_exchange(self):
        try:
            self.exchange = getattr(ccxt, EXCHANGE)({
                "enableRateLimit": True,
                "options": {"defaultType": "spot"},
                "timeout": 20000
            })
            
            ticker = await self.exchange.fetch_ticker("BTC/USDT")
            log.info(f"✅ Exchange connected. BTC: ${ticker['last']:.2f}")
            
        except Exception as e:
            log.error(f"Exchange error: {e}")
            raise
    
    async def _send_startup_message(self):
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            log.warning("⚠️ Telegram credentials not set")
            return
        
        try:
            message = f"""
🧠 <b>COMPLETE TRADER SYSTEM STARTED</b>

<b>🎯 SYSTEM PHILOSOPHY:</b>
Phase 1 → Direction Analysis (7 tools must ALL agree)
Phase 2 → Price Entry (NO indicators, pure price behavior)

<b>📊 7 DIRECTION TOOLS:</b>
1️⃣ Multi-Timeframe Agreement
2️⃣ Wave Length Analysis  
3️⃣ Momentum Strength
4️⃣ Volume Participation
5️⃣ RSI Regime
6️⃣ EMA Structure
7️⃣ Volatility Tradability

<b>⚡ 3 ENTRY TYPES:</b>
• <b>PULLBACK</b> → Strong trends
• <b>BREAKOUT</b> → Fast markets  
• <b>STOPHUNT</b> → Volatile spikes

✅ <b>System is now live and scanning for trades</b>
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML"
                })
                
            log.info("✅ Startup message sent")
                
        except Exception as e:
            log.error(f"Telegram error: {e}")
    
    async def fetch_timeframe_data(self, symbol: str) -> Dict[str, pd.DataFrame]:
        data = {}
        
        for tf_name, tf in TIMEFRAMES.items():
            try:
                if tf_name == "1H":
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
                    
            except Exception:
                continue
        
        return data
    
    async def get_active_pairs(self) -> List[Tuple[str, float]]:
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
    
    async def analyze_symbol(self, symbol: str) -> Optional[TradeSignal]:
        try:
            log.info(f"\n{'='*60}")
            log.info(f"🧠 ANALYZING {symbol}")
            log.info(f"{'='*60}")
            
            # Fetch data
            multi_tf_data = await self.fetch_timeframe_data(symbol)
            
            # Check if we have enough data - NO "if df" checks!
            required_tfs = ["1H", "15M", "5M"]
            has_required_data = True
            
            for tf in required_tfs:
                if tf not in multi_tf_data:
                    has_required_data = False
                    break
                
                df = multi_tf_data[tf]
                if not is_valid_df(df):
                    has_required_data = False
                    break
            
            if not has_required_data:
                log.debug(f"{symbol}: Missing required timeframes")
                return None
            
            # ===== PHASE 1: DIRECTION ANALYSIS =====
            direction_analysis = self.direction_engine.analyze_direction(multi_tf_data)
            
            if direction_analysis.direction == "NO_TRADE":
                log.info(f"❌ {symbol}: No trade direction")
                return None
            
            log.info(f"✅ DIRECTION LOCKED: {direction_analysis.direction}")
            
            # ===== PHASE 2: MARKET STATE =====
            primary_df = None
            if "5M" in multi_tf_data:
                primary_df = multi_tf_data["5M"]
            elif "3M" in multi_tf_data:
                primary_df = multi_tf_data["3M"]
            
            if not is_valid_df(primary_df):
                return None
            
            market_state = detect_market_state(primary_df)
            log.info(f"📊 Market State: {market_state.state}")
            
            # ===== PHASE 3: PRICE ENTRY =====
            log.info(f"\n🎯 SWITCHING TO PRICE-ONLY MODE")
            
            entry_df = None
            if "3M" in multi_tf_data:
                entry_df = multi_tf_data["3M"]
            elif "1M" in multi_tf_data:
                entry_df = multi_tf_data["1M"]
            
            if not is_valid_df(entry_df):
                log.warning("❌ No entry timeframe data")
                return None
            
            price_entry = self.price_entry_engine.find_entry(
                entry_df, 
                direction_analysis.direction,
                market_state
            )
            
            if price_entry is None:
                log.info(f"❌ No price entry for {symbol}")
                return None
            
            # ===== CREATE TRADE SIGNAL =====
            signal_id = hashlib.md5(
                f"{symbol}:{direction_analysis.direction}:{price_entry.entry_type}:{time.time()}".encode()
            ).hexdigest()
            
            risk = abs(price_entry.entry_price - price_entry.stop_loss)
            reward = abs(price_entry.take_profit - price_entry.entry_price)
            risk_reward = reward / risk if risk > 0 else 0
            
            conditions_met = [
                f"DIRECTION_{direction_analysis.direction}",
                f"MARKET_{market_state.state}",
                f"ENTRY_{price_entry.entry_type}",
                f"CONFIDENCE_{direction_analysis.confidence:.2f}"
            ]
            
            signal = TradeSignal(
                signal_id=signal_id,
                symbol=symbol,
                direction=direction_analysis.direction,
                entry=price_entry,
                market_state=market_state,
                direction_analysis=direction_analysis,
                risk_reward=risk_reward,
                conditions_met=conditions_met
            )
            
            # Save to database
            await self.save_trade_signal(signal)
            
            # Send alert
            await self.send_trade_alert(signal)
            
            log.info(f"\n🎯 TRADE SIGNAL GENERATED: {symbol} {direction_analysis.direction}")
            log.info(f"   Entry Type: {price_entry.entry_type}")
            log.info(f"   Entry: {price_entry.entry_price:.4f}")
            log.info(f"   SL: {price_entry.stop_loss:.4f}, TP: {price_entry.take_profit:.4f}")
            log.info(f"   R:R: {risk_reward:.1f}:1")
            
            return signal
            
        except Exception as e:
            log.error(f"Analysis error for {symbol}: {e}")
            return None
    
    async def save_trade_signal(self, signal: TradeSignal) -> bool:
        try:
            await self.db.execute("""
                INSERT INTO trades (
                    id, symbol, direction, entry_type,
                    entry_price, stop_loss, take_profit,
                    market_state, volatility, trend_strength,
                    tool_results, confidence, risk_reward,
                    conditions_met
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal.signal_id,
                signal.symbol,
                signal.direction,
                signal.entry.entry_type,
                signal.entry.entry_price,
                signal.entry.stop_loss,
                signal.entry.take_profit,
                signal.market_state.state,
                signal.market_state.volatility,
                signal.market_state.trend_strength,
                json.dumps(signal.direction_analysis.tool_results),
                signal.direction_analysis.confidence,
                signal.risk_reward,
                json.dumps(signal.conditions_met)
            ))
            
            await self.db.commit()
            log.info(f"✅ Trade saved: {signal.symbol}")
            return True
            
        except Exception as e:
            log.error(f"Error saving trade: {e}")
            return False
    
    async def send_trade_alert(self, signal: TradeSignal):
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return
        
        try:
            side_emoji = "🟢" if signal.direction == "LONG" else "🔴"
            side_text = "شراء" if signal.direction == "LONG" else "بيع"
            
            entry_translation = {
                "PULLBACK": "تراجع",
                "BREAKOUT": "كسر",
                "STOPHUNT": "صيد وقف"
            }
            
            entry_text = entry_translation.get(signal.entry.entry_type, signal.entry.entry_type)
            
            state_translation = {
                "STRONG_TREND": "اتجاه قوي",
                "FAST_MARKET": "سوق سريع",
                "VOLATILE_SPIKES": "تقلبات حادة"
            }
            
            state_text = state_translation.get(signal.market_state.state, signal.market_state.state)
            
            message = f"""
{side_emoji} <b>إشارة تداول جديدة</b> ⚡

<b>{signal.symbol}</b> | {side_text}

<b>🎯 تحليل الاتجاه:</b>
‎• الاتجاه: {side_text}
‎• الثقة: {signal.direction_analysis.confidence:.0%}
‎• حالة السوق: {state_text}

<b>⚡ نوع الدخول:</b>
‎• النوع: {entry_text}

<b>🔧 التنفيذ:</b>
‎• سعر الدخول: <code>{signal.entry.entry_price:.6f}</code>
‎• وقف الخسارة: <code>{signal.entry.stop_loss:.6f}</code>
‎• هدف الربح: <code>{signal.entry.take_profit:.6f}</code>
‎• نسبة الربح/المخاطرة: {signal.risk_reward:.1f}:1

#{side_text} #{entry_text}
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML"
                })
                
            log.info(f"📤 Trade alert sent: {signal.symbol}")
            
        except Exception as e:
            log.error(f"Telegram alert error: {e}")
    
    async def monitor_trades(self):
        log.info("👀 Starting trade monitoring...")
        
        while True:
            try:
                async with self.db.execute("""
                    SELECT id, symbol, direction, entry_price, stop_loss, take_profit, status
                    FROM trades WHERE status = 'PENDING'
                """) as cursor:
                    trades = await cursor.fetchall()
                
                for trade_id, symbol, direction, entry, sl, tp, status in trades:
                    try:
                        ticker = await self.exchange.fetch_ticker(symbol)
                        current_price = ticker['last']
                        
                        if status == 'PENDING':
                            await self.db.execute("""
                                UPDATE trades SET 
                                    status = 'TRIGGERED',
                                    triggered_at = CURRENT_TIMESTAMP,
                                    trigger_price = ?
                                WHERE id = ?
                            """, (current_price, trade_id))
                            
                            await self.db.commit()
                            
                            self.active_trades[trade_id] = {
                                'symbol': symbol,
                                'direction': direction,
                                'entry': entry,
                                'sl': sl,
                                'tp': tp,
                                'triggered_at': time.time()
                            }
                            
                            log.info(f"✅ Trade triggered: {symbol} {direction} @ {current_price:.4f}")
                            continue
                        
                        if trade_id in self.active_trades:
                            trade_data = self.active_trades[trade_id]
                            
                            pnl_percent = 0
                            close_reason = None
                            
                            if direction == "LONG":
                                if current_price <= sl:
                                    close_reason = "SL_HIT"
                                    pnl_percent = ((current_price - entry) / entry) * 100
                                elif current_price >= tp:
                                    close_reason = "TP_HIT"
                                    pnl_percent = ((current_price - entry) / entry) * 100
                            
                            else:
                                if current_price >= sl:
                                    close_reason = "SL_HIT"
                                    pnl_percent = ((entry - current_price) / entry) * 100
                                elif current_price <= tp:
                                    close_reason = "TP_HIT"
                                    pnl_percent = ((entry - current_price) / entry) * 100
                            
                            if close_reason:
                                await self.db.execute("""
                                    UPDATE trades SET 
                                        status = 'CLOSED',
                                        closed_at = CURRENT_TIMESTAMP,
                                        close_price = ?,
                                        pnl_percent = ?,
                                        close_reason = ?
                                    WHERE id = ?
                                """, (current_price, pnl_percent, close_reason, trade_id))
                                
                                await self.db.commit()
                                del self.active_trades[trade_id]
                                
                                log.info(f"{'✅' if close_reason == 'TP_HIT' else '❌'} Trade closed: {symbol} {pnl_percent:+.2f}% ({close_reason})")
                    
                    except Exception as e:
                        log.error(f"Monitor error for {symbol}: {e}")
                        continue
                
                if int(time.time()) % 300 < 2:
                    old_trades = []
                    for trade_id, data in list(self.active_trades.items()):
                        if time.time() - data.get('triggered_at', 0) > 86400:
                            old_trades.append(trade_id)
                    
                    for trade_id in old_trades:
                        del self.active_trades[trade_id]
                
                await asyncio.sleep(3)
                
            except Exception as e:
                log.error(f"Monitoring loop error: {e}")
                await asyncio.sleep(5)
    
    async def scanning_loop(self):
        log.info("🚀 Starting scanning loop...")
        
        cycle = 0
        
        while True:
            try:
                cycle += 1
                start_time = time.time()
                
                log.info(f"\n📊 Scan Cycle #{cycle}")
                
                pairs = await self.get_active_pairs()
                
                if not pairs:
                    log.warning("No active pairs found")
                    await asyncio.sleep(SCAN_INTERVAL)
                    continue
                
                log.info(f"Scanning {len(pairs)} pairs")
                
                signals_found = 0
                
                for symbol, volume in pairs:
                    try:
                        if len(self.active_trades) >= MAX_POSITIONS:
                            log.warning(f"Max positions reached ({MAX_POSITIONS})")
                            break
                        
                        active_symbols = [data['symbol'] for data in self.active_trades.values()]
                        if symbol in active_symbols:
                            continue
                        
                        signal = await self.analyze_symbol(symbol)
                        
                        if signal is not None:
                            signals_found += 1
                        
                        await asyncio.sleep(0.05)
                        
                    except Exception as e:
                        log.debug(f"Pair error {symbol}: {str(e)[:50]}")
                        continue
                
                dir_stats = self.direction_engine.get_stats()
                entry_stats = self.price_entry_engine.get_stats()
                
                scan_duration = time.time() - start_time
                
                log.info(f"\n📈 Cycle #{cycle} Summary:")
                log.info(f"  Signals found: {signals_found}")
                log.info(f"  Active trades: {len(self.active_trades)}/{MAX_POSITIONS}")
                log.info(f"  Direction stats: L:{dir_stats['LONG']} S:{dir_stats['SHORT']} N:{dir_stats['NO_TRADE']}")
                log.info(f"  Entry stats: P:{entry_stats['pullback_entries']} B:{entry_stats['breakout_entries']} S:{entry_stats['stophunt_entries']}")
                log.info(f"  Scan duration: {scan_duration:.2f}s")
                
                wait_time = max(1, SCAN_INTERVAL - scan_duration)
                log.info(f"Next scan in {wait_time:.1f}s...")
                await asyncio.sleep(wait_time)
                
            except Exception as e:
                log.error(f"Scanning loop error: {e}")
                await asyncio.sleep(10)
    
    async def run(self):
        try:
            await self.initialize()
            
            await asyncio.gather(
                self.scanning_loop(),
                self.monitor_trades()
            )
            
        except KeyboardInterrupt:
            log.info("\n🛑 System stopped by user")
        except Exception as e:
            log.error(f"System crashed: {e}")
        finally:
            await self.cleanup()
    
    async def cleanup(self):
        try:
            if self.exchange:
                await self.exchange.close()
                log.info("Exchange closed")
            
            if self.db:
                await self.db.close()
                log.info("Database closed")
                
        except Exception as e:
            log.error(f"Cleanup error: {e}")

# ================ MAIN ================
async def main():
    system = CompleteTraderSystem()
    await system.run()

if __name__ == "__main__":
    asyncio.run(main())