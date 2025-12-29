#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🔥 ADAPTIVE REJECTION SYSTEM
Professional discretionary trading with context-based confirmation
TWO MODES: Fast Confirmation (70%) + Full Confirmation (30%)
TRADER MINDSET: Context-aware, adaptive strictness
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

# ================ ADAPTIVE CONFIG ================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DB_PATH = "/app/data/adaptive_rejection.db"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 5))
TOP_N_VOLUME = int(os.getenv("TOP_N_VOLUME", 100))
MIN_VOLUME_USD = 500000

# Trading parameters
MAX_STOP_LOSS_PCT = 1.0
MIN_TARGET_PCT = 1.5
MAX_TARGET_PCT = 4.0
MIN_RISK_REWARD = 2.0

# ADAPTIVE MODE CONFIG
ADAPTIVE_CONFIG = {
    "fast_mode": {
        "enabled": True,
        "target_ratio": 0.7,
        "min_confirmation_elements": 1,
        "allowed_elements": ["RSI_FAILURE", "VOLUME_WEAKNESS", "EMA_REJECTION"],
        "entry_candle": "REJECTION",
        "win_rate_target": 0.45,
    },
    "full_mode": {
        "enabled": True,
        "trigger_conditions": ["CHOPPY_MARKET", "LATE_WAVE", "HIGH_VOLATILITY"],
        "min_confirmation_elements": 3,
        "required_elements": ["FAILURE_CANDLE", "STRENGTH_SHIFT", "VOLUME_CONFIRMATION"],
        "entry_candle": "STRENGTH_SHIFT",
        "win_rate_target": 0.55,
    },
    "mode_selection": {
        "choppy_market_threshold": 0.4,
        "late_wave_threshold": 0.8,
        "high_volatility_threshold": 0.05,
        "clear_trend_threshold": 0.7,
    }
}

# Timeframes
TIMEFRAMES = {
    "1H": "1h",
    "15M": "15m", 
    "5M": "5m",
    "3M": "3m",
    "1M": "1m"
}

EMA_PERIODS = {"fast": 9, "medium": 21, "slow": 50}
RSI_PERIOD = 14

# ================ DATA STRUCTURES ================
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
class MarketContext:
    trend_clarity: float
    wave_timing: str
    volatility_level: str
    structure_quality: str
    mode_suggestion: str
  
@dataclass
class FastConfirmation:
    has_rsi_failure: bool
    has_volume_weakness: bool
    has_ema_rejection: bool
    elements_present: List[str]
    total_elements: int
    
@dataclass
class FullConfirmation:
    failure_candle_strength: float
    strength_shift_strength: float
    volume_confirmation: bool
    elements_present: List[str]
    is_complete: bool

@dataclass
class AdaptiveConfirmation:
    selected_mode: str
    confirmation_strength: float
    elements_used: List[str]
    entry_candle: str
    reason: str
    
@dataclass
class AdaptiveRejectionSignal:
    signal_id: str
    symbol: str
    side: str
    entry_price: float
    stop_loss: float
    take_profit: float
    wave_context: WaveContext
    market_strength: MarketStrength
    market_context: MarketContext
    rejection_zone: RejectionZone
    confirmation_mode: str
    confirmation: AdaptiveConfirmation
    entry_reason: str
    rejection_strength: float
    risk_reward: float
    expected_move_pct: float
    entry_timeframe: str
    signal_timestamp: float
    passed_non_negotiables: bool

# ================ PROFESSIONAL LOGGING ================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)8s | %(name)s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger("adaptive_rejection_scanner")

# ================ ADAPTIVE REJECTION ENGINE ================
class AdaptiveRejectionScanner:
    def __init__(self):
        self.daily_stats = {
            "total_signals": 0,
            "fast_mode_signals": 0,
            "full_mode_signals": 0,
            "mode_switches": 0,
            "choppy_market_full": 0,
            "late_wave_full": 0,
            "high_vol_full": 0,
            "rejections_no_confirmation": 0,
            "failed_non_negotiables": 0,
            "pairs_scanned": 0
        }
        self.trade_mode_distribution = {"FAST": 0, "FULL": 0}
        self.deduplicator = self.SignalDeduplicator()
        self.active_signal_ids = set()
    
    class SignalDeduplicator:
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
        
        def register_signal(self, signal: AdaptiveRejectionSignal):
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
                "timestamp": signal.signal_timestamp,
                "mode": signal.confirmation_mode
            }
            
            log.debug(f"Registered {signal.confirmation_mode} signal {signal.signal_id[:8]} for {symbol}")
        
        def update_signal_status(self, signal_id: str, status: str):
            if signal_id in self.signal_status:
                self.signal_status[signal_id]["status"] = status
                if status == "CLOSED":
                    symbol = self.signal_status[signal_id]["symbol"]
                    mode = self.signal_status[signal_id].get("mode", "UNKNOWN")
                    log.info(f"✅ {mode} signal {signal_id[:8]} for {symbol} CLOSED")
        
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
    
    # ========== WAVE CONTEXT ANALYSIS ==========
    
    def analyze_wave_context(self, df_1h: pd.DataFrame, df_15m: pd.DataFrame) -> WaveContext:
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
            
        except Exception:
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
            return min(avg_speed / 5.0, 1.0)
            
        except Exception:
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
                
        except Exception:
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
                
        except Exception:
            return "NEUTRAL"
    
    def _get_default_wave_context(self) -> WaveContext:
        return WaveContext(
            wave_length="MEDIUM",
            wave_maturity=0.5,
            expansion_speed=0.5,
            structure_type="COMPRESSION",
            context_side="NEUTRAL"
        )
    
    # ========== MARKET STRENGTH ANALYSIS ==========
    
    def analyze_market_strength(self, df: pd.DataFrame) -> MarketStrength:
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
            
        except Exception:
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
            
        except Exception:
            return 0.5
    
    def _calculate_ema_angle(self, df: pd.DataFrame) -> float:
        try:
            if len(df) < 20:
                return 0.0
            
            ema_fast = df['close'].ewm(span=EMA_PERIODS['fast'], adjust=False).mean()
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
            
        except Exception:
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
            
        except Exception:
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
            
        except Exception:
            return False, False, False, False
    
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
    
    # ========== REJECTION ZONE ANALYSIS ==========
    
    def find_rejection_zones(self, df: pd.DataFrame, current_price: float,
                            rsi_value: float, emas: Dict[str, float]) -> List[RejectionZone]:
        zones = []
        
        try:
            if df is None or len(df) < 20:
                return zones
            
            # EMA zones
            for ema_name, ema_value in emas.items():
                if ema_value == 0:
                    continue
                
                distance_pct = abs(current_price - ema_value) / ema_value * 100
                
                if distance_pct <= 0.5:
                    if current_price > ema_value:
                        zone_type = "EMA_SUPPORT"
                    else:
                        zone_type = "EMA_RESISTANCE"
                    
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
                        rsi_position="NEUTRAL",
                        is_active=True
                    ))
            
            # Range zones
            if len(df) >= 20:
                recent_high = df['high'].values[-20:].max()
                recent_low = df['low'].values[-20:].min()
                
                high_distance_pct = abs(current_price - recent_high) / recent_high * 100
                if high_distance_pct <= 0.3:
                    zones.append(RejectionZone(
                        zone_type="RANGE_HIGH",
                        price_level=recent_high,
                        strength=0.8,
                        volume_confirmation=False,
                        rsi_position="NEUTRAL",
                        is_active=True
                    ))
                
                low_distance_pct = abs(current_price - recent_low) / recent_low * 100
                if low_distance_pct <= 0.3:
                    zones.append(RejectionZone(
                        zone_type="RANGE_LOW",
                        price_level=recent_low,
                        strength=0.8,
                        volume_confirmation=False,
                        rsi_position="NEUTRAL",
                        is_active=True
                    ))
            
            # Failed break zones
            if len(df) >= 10:
                recent_high = df['high'].values[-5:].max()
                prev_high = df['high'].values[-10:-5].max()
                
                if current_price < recent_high and recent_high > prev_high * 1.005:
                    if any(df['close'].values[-5:] < recent_high * 0.995):
                        zones.append(RejectionZone(
                            zone_type="FAILED_BREAKOUT",
                            price_level=recent_high,
                            strength=0.85,
                            volume_confirmation=False,
                            rsi_position="NEUTRAL",
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
                            rsi_position="NEUTRAL",
                            is_active=True
                        ))
            
            # Set RSI position
            for zone in zones:
                zone.rsi_position = self._analyze_rsi_position(rsi_value, zone.zone_type)
            
            return [z for z in zones if z.is_active]
            
        except Exception as e:
            log.error(f"Rejection zone error: {e}")
            return []
    
    def _analyze_rsi_position(self, rsi_value: float, zone_type: str) -> str:
        if "SUPPORT" in zone_type or "LOW" in zone_type or "BREAKDOWN" in zone_type:
            if 40 <= rsi_value <= 50:
                return "IN_ZONE"
            elif rsi_value < 30:
                return "OVEREXTENDED"
            else:
                return "NEUTRAL"
        
        elif "RESISTANCE" in zone_type or "HIGH" in zone_type or "BREAKOUT" in zone_type:
            if 50 <= rsi_value <= 60:
                return "IN_ZONE"
            elif rsi_value > 70:
                return "OVEREXTENDED"
            else:
                return "NEUTRAL"
        
        return "NEUTRAL"
    
    # ========== MARKET CONTEXT ANALYSIS ==========
    
    def analyze_market_context(self, df_1h: pd.DataFrame, df_15m: pd.DataFrame, 
                              df_5m: pd.DataFrame) -> MarketContext:
        try:
            if df_1h is None or df_15m is None or df_5m is None:
                return self._get_default_market_context()
            
            trend_clarity = self._calculate_trend_clarity(df_1h, df_15m)
            wave_timing = self._determine_wave_timing(df_1h, df_15m)
            volatility_level = self._determine_volatility_level(df_5m)
            structure_quality = self._determine_structure_quality(df_15m)
            
            mode_suggestion = self._suggest_trading_mode(
                trend_clarity, wave_timing, volatility_level, structure_quality
            )
            
            return MarketContext(
                trend_clarity=trend_clarity,
                wave_timing=wave_timing,
                volatility_level=volatility_level,
                structure_quality=structure_quality,
                mode_suggestion=mode_suggestion
            )
            
        except Exception as e:
            log.error(f"Market context error: {e}")
            return self._get_default_market_context()
    
    def _calculate_trend_clarity(self, df_1h: pd.DataFrame, df_15m: pd.DataFrame) -> float:
        try:
            if len(df_15m) < 20:
                return 0.5
            
            high_1h = df_1h['high'].values[-10:].max()
            low_1h = df_1h['low'].values[-10:].min()
            range_1h = high_1h - low_1h
            
            if range_1h == 0:
                return 0.3
            
            price_change = abs(df_1h['close'].iloc[-1] - df_1h['close'].iloc[-10])
            efficiency = price_change / range_1h
            
            ema_fast_15m = df_15m['close'].ewm(span=9, adjust=False).mean()
            ema_slow_15m = df_15m['close'].ewm(span=21, adjust=False).mean()
            
            ema_alignment = 1.0 if (ema_fast_15m.iloc[-1] > ema_slow_15m.iloc[-1]) == \
                (ema_fast_15m.iloc[-5] > ema_slow_15m.iloc[-5]) else 0.3
            
            trend_clarity = (efficiency * 0.6 + ema_alignment * 0.4)
            return min(max(trend_clarity, 0), 1)
            
        except Exception:
            return 0.5
    
    def _determine_wave_timing(self, df_1h: pd.DataFrame, df_15m: pd.DataFrame) -> str:
        try:
            wave_context = self.analyze_wave_context(df_1h, df_15m)
            maturity = wave_context.wave_maturity
            
            if maturity < 0.3:
                return "EARLY"
            elif maturity < 0.7:
                return "MIDDLE"
            else:
                return "LATE"
                
        except Exception:
            return "MIDDLE"
    
    def _determine_volatility_level(self, df_5m: pd.DataFrame) -> str:
        try:
            if len(df_5m) < 20:
                return "MEDIUM"
            
            high_low = df_5m['high'] - df_5m['low']
            atr = high_low.rolling(window=14).mean().iloc[-1]
            current_price = df_5m['close'].iloc[-1]
            
            if current_price == 0:
                return "MEDIUM"
            
            atr_pct = (atr / current_price) * 100
            
            if atr_pct < 0.3:
                return "LOW"
            elif atr_pct < 0.5:
                return "MEDIUM"
            else:
                return "HIGH"
                
        except Exception:
            return "MEDIUM"
    
    def _determine_structure_quality(self, df_15m: pd.DataFrame) -> str:
        try:
            if len(df_15m) < 20:
                return "MESSY"
            
            highs = df_15m['high'].values[-10:]
            lows = df_15m['low'].values[-10:]
            
            clear_swings = 0
            for i in range(1, len(highs)):
                if highs[i] > highs[i-1] or lows[i] < lows[i-1]:
                    clear_swings += 1
            
            clean_ratio = clear_swings / 9
            
            overlap_count = 0
            for i in range(1, len(highs)):
                if highs[i] < lows[i-1] or lows[i] > highs[i-1]:
                    overlap_count += 1
            
            if clean_ratio > 0.7 and overlap_count < 3:
                return "CLEAN"
            else:
                return "MESSY"
                
        except Exception:
            return "MESSY"
    
    def _suggest_trading_mode(self, trend_clarity: float, wave_timing: str,
                             volatility_level: str, structure_quality: str) -> str:
        mode = "FAST"
        
        if trend_clarity < ADAPTIVE_CONFIG["mode_selection"]["choppy_market_threshold"]:
            mode = "FULL"
            self.daily_stats["choppy_market_full"] += 1
        
        if wave_timing == "LATE":
            mode = "FULL"
            self.daily_stats["late_wave_full"] += 1
        
        if volatility_level == "HIGH":
            mode = "FULL"
            self.daily_stats["high_vol_full"] += 1
        
        if structure_quality == "MESSY" and mode == "FULL":
            mode = "FULL"
        
        total_signals = self.trade_mode_distribution["FAST"] + self.trade_mode_distribution["FULL"]
        if total_signals > 10:
            full_ratio = self.trade_mode_distribution["FULL"] / total_signals
            if full_ratio > 0.4 and mode == "FULL" and not any([
                trend_clarity < 0.4,
                wave_timing == "LATE",
                volatility_level == "HIGH"
            ]):
                mode = "FAST"
        
        return mode
    
    def _get_default_market_context(self) -> MarketContext:
        return MarketContext(
            trend_clarity=0.5,
            wave_timing="MIDDLE",
            volatility_level="MEDIUM",
            structure_quality="MESSY",
            mode_suggestion="FAST"
        )
    
    # ========== FAST CONFIRMATION DETECTION ==========
    
    def detect_fast_confirmation(self, df: pd.DataFrame, zone: RejectionZone,
                                side: str, current_rsi: float, prev_rsi: float,
                                emas: Dict[str, float]) -> FastConfirmation:
        elements_present = []
        
        has_rsi_failure = self._check_rsi_failure(current_rsi, prev_rsi, side)
        if has_rsi_failure:
            elements_present.append("RSI_FAILURE")
        
        has_volume_weakness = self._check_volume_weakness(df, side)
        if has_volume_weakness:
            elements_present.append("VOLUME_WEAKNESS")
        
        has_ema_rejection = self._check_ema_rejection(df, zone, emas, side)
        if has_ema_rejection:
            elements_present.append("EMA_REJECTION")
        
        return FastConfirmation(
            has_rsi_failure=has_rsi_failure,
            has_volume_weakness=has_volume_weakness,
            has_ema_rejection=has_ema_rejection,
            elements_present=elements_present,
            total_elements=len(elements_present)
        )
    
    def _check_rsi_failure(self, current_rsi: float, prev_rsi: float, side: str) -> bool:
        if side == "LONG":
            rsi_slope = current_rsi - prev_rsi
            if rsi_slope > 0:
                return True
            elif current_rsi < 35 and prev_rsi < 30:
                return True
        else:
            rsi_slope = current_rsi - prev_rsi
            if rsi_slope < 0:
                return True
            elif current_rsi > 65 and prev_rsi > 70:
                return True
        return False
    
    def _check_volume_weakness(self, df: pd.DataFrame, side: str) -> bool:
        try:
            if len(df) < 5:
                return False
            
            current_candle = df.iloc[-1]
            
            if side == "LONG":
                if current_candle['close'] < current_candle['open']:
                    current_volume = current_candle['volume']
                    prev_volume = df['volume'].iloc[-5:-1].mean()
                    if prev_volume > 0:
                        ratio = current_volume / prev_volume
                        if ratio < 0.8:
                            return True
            else:
                if current_candle['close'] > current_candle['open']:
                    current_volume = current_candle['volume']
                    prev_volume = df['volume'].iloc[-5:-1].mean()
                    if prev_volume > 0:
                        ratio = current_volume / prev_volume
                        if ratio < 0.8:
                            return True
            return False
        except Exception:
            return False
    
    def _check_ema_rejection(self, df: pd.DataFrame, zone: RejectionZone,
                            emas: Dict[str, float], side: str) -> bool:
        try:
            current_candle = df.iloc[-1]
            candle_range = current_candle['high'] - current_candle['low']
            
            if candle_range == 0:
                return False
            
            if "EMA" in zone.zone_type:
                if side == "LONG":
                    lower_wick = min(current_candle['open'], current_candle['close']) - current_candle['low']
                    wick_ratio = lower_wick / candle_range
                    if wick_ratio > 0.3 and current_candle['low'] < zone.price_level:
                        return True
                else:
                    upper_wick = current_candle['high'] - max(current_candle['open'], current_candle['close'])
                    wick_ratio = upper_wick / candle_range
                    if wick_ratio > 0.3 and current_candle['high'] > zone.price_level:
                        return True
            return False
        except Exception:
            return False
    
    # ========== FULL CONFIRMATION DETECTION ==========
    
    def detect_full_confirmation(self, df_3m: pd.DataFrame, df_1m: pd.DataFrame,
                                zone: RejectionZone, side: str,
                                current_rsi_3m: float, prev_rsi_3m: float,
                                current_rsi_1m: float, prev_rsi_1m: float) -> FullConfirmation:
        elements_present = []
        
        failure_candle_strength = self._analyze_failure_candle_strength(df_3m, zone, side)
        if failure_candle_strength > 0.7:
            elements_present.append("FAILURE_CANDLE")
        
        strength_shift_strength = self._analyze_strength_shift(df_1m, side)
        if strength_shift_strength > 0.7:
            elements_present.append("STRENGTH_SHIFT")
        
        volume_confirmation = self._check_full_volume_confirmation(df_3m, df_1m, side)
        if volume_confirmation:
            elements_present.append("VOLUME_CONFIRMATION")
        
        required = ADAPTIVE_CONFIG["full_mode"]["required_elements"]
        is_complete = all(elem in elements_present for elem in required)
        
        return FullConfirmation(
            failure_candle_strength=failure_candle_strength,
            strength_shift_strength=strength_shift_strength,
            volume_confirmation=volume_confirmation,
            elements_present=elements_present,
            is_complete=is_complete
        )
    
    def _analyze_failure_candle_strength(self, df: pd.DataFrame, zone: RejectionZone, side: str) -> float:
        try:
            current_candle = df.iloc[-1]
            candle_range = current_candle['high'] - current_candle['low']
            
            if candle_range == 0:
                return 0
            
            if side == "LONG":
                lower_wick = min(current_candle['open'], current_candle['close']) - current_candle['low']
                wick_ratio = lower_wick / candle_range
                close_above = current_candle['close'] > zone.price_level * 1.001
                strength = (wick_ratio * 0.6 + (1.0 if close_above else 0.3) * 0.4)
                return min(strength, 1.0)
            else:
                upper_wick = current_candle['high'] - max(current_candle['open'], current_candle['close'])
                wick_ratio = upper_wick / candle_range
                close_below = current_candle['close'] < zone.price_level * 0.999
                strength = (wick_ratio * 0.6 + (1.0 if close_below else 0.3) * 0.4)
                return min(strength, 1.0)
        except Exception:
            return 0
    
    def _analyze_strength_shift(self, df: pd.DataFrame, side: str) -> float:
        try:
            current_candle = df.iloc[-1]
            prev_candle = df.iloc[-2]
            
            if side == "LONG":
                is_bullish = current_candle['close'] > current_candle['open']
                if not is_bullish:
                    return 0
            else:
                is_bearish = current_candle['close'] < current_candle['open']
                if not is_bearish:
                    return 0
            
            candle_range = current_candle['high'] - current_candle['low']
            if candle_range == 0:
                return 0
            
            body_size = abs(current_candle['close'] - current_candle['open'])
            body_ratio = body_size / candle_range
            
            volume_ratio = current_candle['volume'] / prev_candle['volume'] if prev_candle['volume'] > 0 else 1
            volume_score = min(volume_ratio / 1.5, 1.0)
            
            strength = (body_ratio * 0.6 + volume_score * 0.4)
            return min(strength, 1.0)
        except Exception:
            return 0
    
    def _check_full_volume_confirmation(self, df_3m: pd.DataFrame, df_1m: pd.DataFrame, side: str) -> bool:
        try:
            volume_1m = df_1m['volume'].iloc[-1]
            volume_3m = df_3m['volume'].iloc[-1]
            
            if volume_3m > 0:
                ratio = volume_1m / volume_3m
                if ratio >= 1.3:
                    return True
            
            avg_1m = df_1m['volume'].iloc[-5:].mean()
            if avg_1m > 0:
                ratio = volume_1m / avg_1m
                if ratio >= 1.2:
                    return True
            
            return False
        except Exception:
            return False
    
    # ========== NON-NEGOTIABLE CHECKS ==========
    
    def check_non_negotiables(self, df_1h: pd.DataFrame, df_entry: pd.DataFrame,
                             zone: RejectionZone, side: str) -> Tuple[bool, List[str]]:
        failed_rules = []
        
        distance_pct = abs(df_entry['close'].iloc[-1] - zone.price_level) / zone.price_level * 100
        if distance_pct > 0.5:
            failed_rules.append("NOT_AT_REJECTION_ZONE")
        
        current_candle = df_entry.iloc[-1]
        has_rejection = False
        if side == "LONG":
            if (current_candle['low'] < zone.price_level and 
                current_candle['close'] > zone.price_level):
                has_rejection = True
        else:
            if (current_candle['high'] > zone.price_level and 
                current_candle['close'] < zone.price_level):
                has_rejection = True
        
        if not has_rejection:
            failed_rules.append("NO_REJECTION")
        
        if df_1h is not None and len(df_1h) > 10:
            ma_50 = df_1h['close'].rolling(window=50).mean().iloc[-1]
            current_price_1h = df_1h['close'].iloc[-1]
            
            if side == "LONG" and current_price_1h < ma_50 * 0.98:
                failed_rules.append("AGAINST_HTF_BIAS")
            elif side == "SHORT" and current_price_1h > ma_50 * 1.02:
                failed_rules.append("AGAINST_HTF_BIAS")
        
        if len(df_entry) >= 5:
            recent_volume = df_entry['volume'].iloc[-1]
            avg_volume = df_entry['volume'].iloc[-5:].mean()
            
            if recent_volume < avg_volume * 0.5:
                failed_rules.append("NO_VOLUME_READ")
        
        passed = len(failed_rules) == 0
        if not passed:
            self.daily_stats["failed_non_negotiables"] += 1
        
        return passed, failed_rules
    
    # ========== ADAPTIVE CONFIRMATION SELECTION ==========
    
    def select_confirmation_mode(self, market_context: MarketContext,
                                fast_confirmation: FastConfirmation,
                                full_confirmation: FullConfirmation) -> Optional[AdaptiveConfirmation]:
        suggested_mode = market_context.mode_suggestion
        elements_used = []
        entry_candle = "REJECTION"
        reason = ""
        
        if suggested_mode == "FAST":
            if fast_confirmation.total_elements >= ADAPTIVE_CONFIG["fast_mode"]["min_confirmation_elements"]:
                selected_mode = "FAST"
                elements_used = fast_confirmation.elements_present
                entry_candle = ADAPTIVE_CONFIG["fast_mode"]["entry_candle"]
                reason = f"FAST_MODE: {', '.join(elements_used)}"
            else:
                selected_mode = "FULL"
                elements_used = full_confirmation.elements_present
                entry_candle = ADAPTIVE_CONFIG["full_mode"]["entry_candle"]
                reason = "FALLBACK_TO_FULL: Insufficient fast confirmation"
                self.daily_stats["mode_switches"] += 1
        else:
            if full_confirmation.is_complete:
                selected_mode = "FULL"
                elements_used = full_confirmation.elements_present
                entry_candle = ADAPTIVE_CONFIG["full_mode"]["entry_candle"]
                reason = f"FULL_MODE: {market_context.mode_suggestion} context"
            else:
                if fast_confirmation.total_elements >= 2:
                    selected_mode = "FAST"
                    elements_used = fast_confirmation.elements_present
                    entry_candle = ADAPTIVE_CONFIG["fast_mode"]["entry_candle"]
                    reason = "FALLBACK_TO_FAST: Full confirmation not available"
                    self.daily_stats["mode_switches"] += 1
                else:
                    return None
        
        if selected_mode == "FAST":
            strength = len(elements_used) / len(ADAPTIVE_CONFIG["fast_mode"]["allowed_elements"])
        else:
            strength = len(elements_used) / len(ADAPTIVE_CONFIG["full_mode"]["required_elements"])
        
        return AdaptiveConfirmation(
            selected_mode=selected_mode,
            confirmation_strength=strength,
            elements_used=elements_used,
            entry_candle=entry_candle,
            reason=reason
        )
    
    # ========== INDICATOR CALCULATIONS ==========
    
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
            for name, period in EMA_PERIODS.items():
                ema_series = df['close'].ewm(span=period, adjust=False).mean()
                emas[name] = ema_series.iloc[-1] if len(ema_series) > 0 else 0
            return emas
        except Exception:
            return {name: 0 for name in EMA_PERIODS.keys()}
    
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
        
        if zone.rsi_position == "IN_ZONE":
            rsi_score = 0.9
        elif zone.rsi_position == "OVEREXTENDED":
            rsi_score = 0.3
        else:
            rsi_score = 0.5
        
        factors.append(rsi_score)
        weights.append(0.25)
        
        return np.average(factors, weights=weights)
    
    # ========== SIGNAL GENERATION ==========
    
    def generate_adaptive_signal(self, multi_tf_data: Dict[str, pd.DataFrame],
                                symbol: str) -> Optional[AdaptiveRejectionSignal]:
        try:
            tf_1h = multi_tf_data.get("1H")
            tf_15m = multi_tf_data.get("15M")
            tf_5m = multi_tf_data.get("5M")
            tf_3m = multi_tf_data.get("3M")
            tf_1m = multi_tf_data.get("1M")
            
            if tf_3m is None or tf_1m is None:
                return None
            
            market_context = self.analyze_market_context(tf_1h, tf_15m, tf_5m)
            wave_context = self.analyze_wave_context(tf_1h, tf_15m)
            market_strength = self.analyze_market_strength(tf_15m)
            
            if market_strength.strength_score < 0.3:
                return None
            
            current_price_3m = tf_3m['close'].iloc[-1]
            emas_3m = self.calculate_emas(tf_3m)
            
            rsi_series_3m = self.calculate_rsi(tf_3m['close'])
            current_rsi_3m = rsi_series_3m.iloc[-1] if len(rsi_series_3m) > 0 else 50
            prev_rsi_3m = rsi_series_3m.iloc[-2] if len(rsi_series_3m) > 1 else 50
            
            rejection_zones = self.find_rejection_zones(tf_3m, current_price_3m, current_rsi_3m, emas_3m)
            
            if not rejection_zones:
                return None
            
            best_zone = max(rejection_zones, key=lambda z: z.strength)
            
            side = None
            if best_zone.zone_type in ["EMA_SUPPORT", "RANGE_LOW", "FAILED_BREAKDOWN"]:
                side = "LONG"
            elif best_zone.zone_type in ["EMA_RESISTANCE", "RANGE_HIGH", "FAILED_BREAKOUT"]:
                side = "SHORT"
            
            if not side:
                return None
            
            rsi_series_1m = self.calculate_rsi(tf_1m['close'])
            current_rsi_1m = rsi_series_1m.iloc[-1] if len(rsi_series_1m) > 0 else 50
            prev_rsi_1m = rsi_series_1m.iloc[-2] if len(rsi_series_1m) > 1 else 50
            
            fast_confirmation = self.detect_fast_confirmation(
                tf_3m, best_zone, side, current_rsi_3m, prev_rsi_3m, emas_3m
            )
            
            full_confirmation = self.detect_full_confirmation(
                tf_3m, tf_1m, best_zone, side,
                current_rsi_3m, prev_rsi_3m,
                current_rsi_1m, prev_rsi_1m
            )
            
            adaptive_confirmation = self.select_confirmation_mode(
                market_context, fast_confirmation, full_confirmation
            )
            
            if not adaptive_confirmation:
                self.daily_stats["rejections_no_confirmation"] += 1
                return None
            
            entry_tf = tf_1m if adaptive_confirmation.selected_mode == "FULL" else tf_3m
            passed_nn, failed_rules = self.check_non_negotiables(tf_1h, entry_tf, best_zone, side)
            
            if not passed_nn:
                log.debug(f"{symbol}: Failed non-negotiables: {failed_rules}")
                return None
            
            entry_price = tf_1m['close'].iloc[-1] if adaptive_confirmation.selected_mode == "FULL" else tf_3m['close'].iloc[-1]
            if not self.deduplicator.should_generate_signal(symbol, side, entry_price):
                return None
            
            if side == "LONG":
                if adaptive_confirmation.selected_mode == "FAST":
                    stop_loss = entry_price * (1 - 0.008)
                else:
                    stop_loss = entry_price * (1 - 0.005)
                
                target_pct = np.random.uniform(MIN_TARGET_PCT, MAX_TARGET_PCT)
                take_profit = entry_price * (1 + target_pct / 100)
            else:
                if adaptive_confirmation.selected_mode == "FAST":
                    stop_loss = entry_price * (1 + 0.008)
                else:
                    stop_loss = entry_price * (1 + 0.005)
                
                target_pct = np.random.uniform(MIN_TARGET_PCT, MAX_TARGET_PCT)
                take_profit = entry_price * (1 - target_pct / 100)
            
            risk = abs(entry_price - stop_loss)
            reward = abs(take_profit - entry_price)
            if risk == 0:
                return None
            
            risk_reward = reward / risk
            if risk_reward < MIN_RISK_REWARD:
                return None
            
            signal_id = hashlib.md5(
                f"{symbol}:{side}:{entry_price:.8f}:{adaptive_confirmation.selected_mode}".encode()
            ).hexdigest()
            
            rejection_strength = self._calculate_rejection_strength(
                best_zone, market_strength, wave_context, current_rsi_3m
            )
            
            if adaptive_confirmation.selected_mode == "FAST":
                entry_reason = f"FAST: {', '.join(adaptive_confirmation.elements_used)}"
            else:
                entry_reason = f"FULL: {adaptive_confirmation.reason.split(':')[0]}"
            
            signal = AdaptiveRejectionSignal(
                signal_id=signal_id,
                symbol=symbol,
                side=side,
                entry_price=entry_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
                wave_context=wave_context,
                market_strength=market_strength,
                market_context=market_context,
                rejection_zone=best_zone,
                confirmation_mode=adaptive_confirmation.selected_mode,
                confirmation=adaptive_confirmation,
                entry_reason=entry_reason,
                rejection_strength=rejection_strength,
                risk_reward=risk_reward,
                expected_move_pct=target_pct,
                entry_timeframe="1M" if adaptive_confirmation.selected_mode == "FULL" else "3M",
                signal_timestamp=time.time(),
                passed_non_negotiables=True
            )
            
            self.deduplicator.register_signal(signal)
            self.active_signal_ids.add(signal_id)
            self.trade_mode_distribution[adaptive_confirmation.selected_mode] += 1
            
            self.daily_stats["total_signals"] += 1
            if adaptive_confirmation.selected_mode == "FAST":
                self.daily_stats["fast_mode_signals"] += 1
            else:
                self.daily_stats["full_mode_signals"] += 1
            
            log.info(f"🎯 ADAPTIVE {adaptive_confirmation.selected_mode}: {symbol} {side} @ {entry_price:.4f}")
            log.info(f"   Context: {market_context.mode_suggestion} → {adaptive_confirmation.selected_mode}")
            log.info(f"   Elements: {', '.join(adaptive_confirmation.elements_used)}")
            log.info(f"   R:R: {risk_reward:.1f}:1")
            
            return signal
            
        except Exception as e:
            log.error(f"Adaptive signal error for {symbol}: {e}")
            return None
    
    def get_daily_stats(self) -> Dict:
        stats = self.daily_stats.copy()
        total = max(1, self.daily_stats["total_signals"])
        stats.update({
            "mode_distribution": self.trade_mode_distribution,
            "fast_mode_ratio": self.trade_mode_distribution["FAST"] / total,
            "full_mode_ratio": self.trade_mode_distribution["FULL"] / total
        })
        return stats
    
    def cleanup_old_signals(self):
        self.deduplicator.remove_closed_signals()

# ================ MAIN SCANNER SYSTEM ================
class AdaptiveRejectionScannerSystem:
    def __init__(self):
        self.scanner = AdaptiveRejectionScanner()
        self.exchange = None
        self.db = None
        self.scan_cycle = 0
    
    async def initialize(self):
        log.info("=" * 70)
        log.info("🔥 ADAPTIVE REJECTION SYSTEM")
        log.info("=" * 70)
        log.info("TRADER PHILOSOPHY: Context-aware, adaptive strictness")
        log.info("TWO MODES: Fast Confirmation (70%) + Full Confirmation (30%)")
        log.info(f"SCAN INTERVAL: {SCAN_INTERVAL} seconds")
        log.info("=" * 70)
        
        await self._init_database()
        await self._init_exchange()
        await self._send_startup_message()
    
    async def _init_database(self):
        try:
            os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
            self.db = await aiosqlite.connect(DB_PATH)
            
            await self.db.execute("""
            CREATE TABLE IF NOT EXISTS adaptive_signals (
                id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                confirmation_mode TEXT NOT NULL,
                entry_price REAL NOT NULL,
                stop_loss REAL NOT NULL,
                take_profit REAL NOT NULL,
                trend_clarity REAL NOT NULL,
                wave_timing TEXT NOT NULL,
                volatility_level TEXT NOT NULL,
                zone_type TEXT NOT NULL,
                confirmation_elements TEXT NOT NULL,
                confirmation_strength REAL NOT NULL,
                risk_reward REAL NOT NULL,
                expected_move REAL NOT NULL,
                market_context TEXT,
                entry_reason TEXT,
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
            
            await self.db.commit()
            log.info("✅ Adaptive database initialized")
            
        except Exception as e:
            log.error(f"Database error: {e}")
            raise
    
    async def _init_exchange(self):
        try:
            self.exchange = ccxt.okx({
                "enableRateLimit": True,
                "options": {"defaultType": "spot"},
                "timeout": 20000,
                "rateLimit": 50
            })
            
            ticker = await self.exchange.fetch_ticker("BTC/USDT")
            log.info(f"✅ Exchange connected. BTC: ${ticker['last']:.2f}")
            
        except Exception as e:
            log.error(f"Exchange error: {e}")
            raise
    
    async def _send_startup_message(self):
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return
        
        try:
            message = f"""
🎯 <b>نظام الرفض المتكيف - ADAPTIVE REJECTION SYSTEM</b>

<b>🧠 الفلسفة المهنية:</b>
لا نستخدم قاعدة واحدة لكل الأسواق
التأكيد يتكيف مع السياق

<b>⚡ نظامين للتأكيد:</b>

<b>🟢 النمط السريع (70% من الصفقات):</b>
• الدخول مباشرة بعد شمعة الرفض
• يحتاج علامة فشل واحدة فقط
• نسبة ربح مستهدفة: 45–50%

<b>🔵 النمط الكامل (30% من الصفقات):</b>
• الدخول بعد شمعة تحول القوة
• يحتاج جميع علامات التأكيد
• نسبة ربح مستهدفة: 55–60%

<b>🧠 قاعدة الاختيار:</b>
<b>استخدم النمط السريع إذا:</b>
• الاتجاه واضح
• القوة ضعفت عند المستوى
• الفوليوم لا يدعم الاختراق

<b>استخدم النمط الكامل إذا:</b>
• الاتجاه غير واضح
• الموجة متأخرة
• التقلبات عالية

<b>🚫 القواعد غير القابلة للتفاوض:</b>
1. ❌ لا صفقات في المنتصف
2. ❌ لا صفقات بدون رفض
3. ❌ لا صفقات ضد توجه HTF
4. ❌ لا صفقات بدون قراءة فوليوم

#متداول_متكيف #رفض_مرن #نظامان_في_واحد
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML"
                })
                
            log.info("✅ Adaptive startup message sent")
                
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
                    
            except Exception as e:
                log.debug(f"{symbol} {tf_name}: {str(e)[:50]}")
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
    
    async def save_signal(self, signal: AdaptiveRejectionSignal) -> bool:
        try:
            await self.db.execute("""
                INSERT INTO adaptive_signals (
                    id, symbol, side, confirmation_mode,
                    entry_price, stop_loss, take_profit,
                    trend_clarity, wave_timing, volatility_level,
                    zone_type, confirmation_elements, confirmation_strength,
                    risk_reward, expected_move,
                    market_context, entry_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal.signal_id,
                signal.symbol,
                signal.side,
                signal.confirmation_mode,
                signal.entry_price,
                signal.stop_loss,
                signal.take_profit,
                signal.market_context.trend_clarity,
                signal.market_context.wave_timing,
                signal.market_context.volatility_level,
                signal.rejection_zone.zone_type,
                json.dumps(signal.confirmation.elements_used),
                signal.confirmation.confirmation_strength,
                signal.risk_reward,
                signal.expected_move_pct,
                json.dumps({
                    "mode_suggestion": signal.market_context.mode_suggestion,
                    "structure_quality": signal.market_context.structure_quality
                }),
                signal.entry_reason
            ))
            
            await self.db.commit()
            log.info(f"✅ Adaptive signal saved: {signal.symbol}")
            return True
            
        except Exception as e:
            log.error(f"Error saving signal: {e}")
            return False
    
    async def format_signal_message(self, signal: AdaptiveRejectionSignal) -> str:
        side_emoji = "🟢" if signal.side == "LONG" else "🔴"
        side_text = "شراء" if signal.side == "LONG" else "بيع"
        
        zone_translation = {
            "EMA_SUPPORT": "دعم المتوسط المتحرك",
            "EMA_RESISTANCE": "مقاومة المتوسط المتحرك",
            "RANGE_LOW": "قاع النطاق",
            "RANGE_HIGH": "سقف النطاق",
            "FAILED_BREAKDOWN": "اختراق فاشل للأسفل",
            "FAILED_BREAKOUT": "اختراق فاشل للأعلى"
        }
        
        zone_text = zone_translation.get(signal.rejection_zone.zone_type, signal.rejection_zone.zone_type)
        
        risk_pct = abs(signal.entry_price - signal.stop_loss) / signal.entry_price * 100
        
        mode_text = "سريع" if signal.confirmation_mode == "FAST" else "كامل"
        mode_emoji = "⚡" if signal.confirmation_mode == "FAST" else "🛡️"
        
        context_text = ""
        if signal.market_context.mode_suggestion == "FULL":
            context_text = f"\n<b>📊 السياق:</b> {signal.market_context.wave_timing} موجة، {signal.market_context.volatility_level} تقلب"
        
        message = f"""
{side_emoji} <b>إشارة رفض متكيفة</b> {mode_emoji}

<b>{signal.symbol}</b> | {side_text} | النمط: <b>{mode_text}</b>

<b>🎯 تأكيد الدخول:</b>
• النمط: {signal.confirmation_mode}
• العناصر: {', '.join(signal.confirmation.elements_used)}
• القوة: {signal.confirmation.confirmation_strength:.1%}
{context_text}
<b>📍 منطقة الرفض:</b>
• النوع: {zone_text}
• القوة: {signal.rejection_zone.strength:.1%}

<b>💪 تحليل القوة:</b>
• درجة القوة: {signal.market_strength.strength_score:.1%}
• سرعة الشموع: {signal.market_strength.candle_speed:.1%}

<b>🔧 التنفيذ:</b>
• سعر الدخول: <code>{signal.entry_price:.6f}</code>
• وقف الخسارة: <code>{signal.stop_loss:.6f}</code> ({risk_pct:.2f}%)
• هدف الربح: <code>{signal.take_profit:.6f}</code> ({signal.expected_move_pct:.1f}%)
• نسبة الربح/المخاطرة: {signal.risk_reward:.1f}:1

<b>🧠 فلسفة التاجر:</b>
الدخول {signal.entry_reason}
تم تطبيق جميع القواعد غير القابلة للتفاوض
{("دخول سريع عند أول رفض" if signal.confirmation_mode == "FAST" else "دخول متأكد بعد تحول القوة")}

#{side_text} #رفض_متكيف #{mode_text} #صفقة_واحدة
"""
        return message
    
    async def send_telegram_alert(self, signal: AdaptiveRejectionSignal):
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            log.warning(f"⚠️ Telegram credentials missing. Skipping alert for {signal.symbol}")
            return
        
        try:
            message = await self.format_signal_message(signal)
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML"
                })
                
            log.info(f"📤 Telegram adaptive alert sent: {signal.symbol}")
            
        except Exception as e:
            log.error(f"Telegram error: {e}")
    
    async def send_trade_trigger_notification(self, symbol: str, side: str, entry_price: float, mode: str):
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return
        
        try:
            side_emoji = "🟢" if side == "LONG" else "🔴"
            side_text = "شراء" if side == "LONG" else "بيع"
            mode_text = "سريع" if mode == "FAST" else "كامل"
            
            message = f"""
{side_emoji} <b>تم تنفيذ صفقة رفض {mode_text}</b>

<b>{symbol}</b> | {side_text}

<b>🎯 تم الدخول:</b>
<code>{entry_price:.6f}</code>

<b>🛡️ نظام التكرار:</b>
❌ <b>ممنوع</b> إرسال إشارات جديدة لـ {symbol}
✅ مسموح بإشارات جديدة بعد إغلاق هذه الصفقة

#{side_text} #تنفيذ_رفض_{mode_text} #متابعة
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML"
                })
            
            log.info(f"{side_emoji} {mode} trade triggered: {symbol}")
            
        except Exception as e:
            log.error(f"Trigger notification error: {e}")
    
    async def send_trade_close_notification(self, symbol: str, side: str, pnl_percent: float,
                                           close_reason: str, entry_price: float,
                                           close_price: float, risk_reward: float, mode: str):
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return
        
        try:
            if close_reason == "TP_HIT":
                emoji = "✅"
                result_text = "هدف الربح"
                pnl_emoji = "💰"
            else:
                emoji = "❌"
                result_text = "وقف الخسارة"
                pnl_emoji = "💸"
            
            side_text = "شراء" if side == "LONG" else "بيع"
            pnl_formatted = f"+{pnl_percent:.2f}%" if pnl_percent > 0 else f"{pnl_percent:.2f}%"
            
            message = f"""
{emoji} <b>تم إغلاق صفقة رفض</b>

<b>{symbol}</b> | {side_text} | النمط: {mode}

{pnl_emoji} <b>النتيجة: {result_text}</b>
<b>النسبة: {pnl_formatted}</b>

<b>📊 التفاصيل:</b>
• سعر الدخول: <code>{entry_price:.6f}</code>
• سعر الإغلاق: <code>{close_price:.6f}</code>
• الربح/الخسارة: <b>{pnl_formatted}</b>

<b>🛡️ نظام التكرار:</b>
✅ <b>مسموح الآن</b> بإرسال إشارات جديدة لـ {symbol}

#{side_text} #إغلاق_رفض #{"ربح" if close_reason == "TP_HIT" else "خسارة"} #مسموح_إشارات_جديدة
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML"
                })
            
            log.info(f"{emoji} {mode} trade closed: {symbol} {pnl_formatted}")
            
        except Exception as e:
            log.error(f"Close notification error: {e}")
    
    async def monitor_positions(self):
        log.info("👀 Starting adaptive position monitoring...")
        
        while True:
            try:
                async with self.db.execute("""
                    SELECT id, symbol, side, entry_price, stop_loss, take_profit, status, confirmation_mode
                    FROM adaptive_signals 
                    WHERE status IN ('PENDING', 'TRIGGERED')
                """) as cursor:
                    positions = await cursor.fetchall()
                
                if positions:
                    log.debug(f"📊 Monitoring {len(positions)} open positions")
                
                for pos_id, symbol, side, entry, sl, tp, status, mode in positions:
                    try:
                        ticker = await self.exchange.fetch_ticker(symbol)
                        current_price = ticker['last']
                        
                        if status == 'PENDING':
                            if abs(current_price - entry) / entry <= 0.005:
                                await self.db.execute("""
                                    UPDATE adaptive_signals SET 
                                        status = 'TRIGGERED',
                                        triggered_at = CURRENT_TIMESTAMP,
                                        trigger_price = ?
                                    WHERE id = ?
                                """, (current_price, pos_id))
                                
                                await self.db.commit()
                                self.scanner.deduplicator.update_signal_status(pos_id, "TRIGGERED")
                                await self.send_trade_trigger_notification(symbol, side, current_price, mode)
                                log.info(f"✅ {mode} position triggered: {symbol}")
                                continue
                        
                        pnl_percent = 0
                        close_reason = None
                        
                        if side == "LONG":
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
                                UPDATE adaptive_signals SET 
                                    status = 'CLOSED',
                                    closed_at = CURRENT_TIMESTAMP,
                                    close_price = ?,
                                    pnl_percent = ?,
                                    close_reason = ?
                                WHERE id = ?
                            """, (current_price, pnl_percent, close_reason, pos_id))
                            
                            await self.db.commit()
                            self.scanner.deduplicator.update_signal_status(pos_id, "CLOSED")
                            self.scanner.active_signal_ids.discard(pos_id)
                            
                            await self.send_trade_close_notification(
                                symbol=symbol,
                                side=side,
                                pnl_percent=pnl_percent,
                                close_reason=close_reason,
                                entry_price=entry,
                                close_price=current_price,
                                risk_reward=0,  # Can fetch from DB if needed
                                mode=mode
                            )
                    
                    except Exception as e:
                        log.error(f"Monitor error for {symbol}: {e}")
                        continue
                
                if int(time.time()) % 300 < 2:
                    self.scanner.deduplicator.remove_closed_signals()
                
                await asyncio.sleep(2)
                
            except Exception as e:
                log.error(f"Monitoring loop error: {e}")
                await asyncio.sleep(5)
    
    async def adaptive_scanning(self):
        log.info("🚀 Starting adaptive rejection scanning...")
        
        while True:
            try:
                self.scan_cycle += 1
                start_time = time.time()
                
                log.info(f"🔄 Adaptive scan #{self.scan_cycle}")
                
                pairs = await self.get_active_pairs()
                
                if not pairs:
                    await asyncio.sleep(SCAN_INTERVAL)
                    continue
                
                log.info(f"Scanning {len(pairs)} pairs with adaptive confirmation")
                
                signals_found = 0
                
                for symbol, volume in pairs:
                    try:
                        multi_tf_data = await self.fetch_timeframe_data(symbol)
                        
                        required_tfs = ["1H", "15M", "5M", "3M", "1M"]
                        has_all_data = all(tf in multi_tf_data for tf in required_tfs)
                        
                        if not has_all_data:
                            continue
                        
                        signal = self.scanner.generate_adaptive_signal(multi_tf_data, symbol)
                        
                        if signal:
                            saved = await self.save_signal(signal)
                            
                            if saved:
                                await self.send_telegram_alert(signal)
                                signals_found += 1
                        
                        self.scanner.daily_stats["pairs_scanned"] += 1
                        await asyncio.sleep(0.01)
                        
                    except Exception as e:
                        log.debug(f"Pair error {symbol}: {str(e)[:50]}")
                        continue
                
                stats = self.scanner.get_daily_stats()
                active_count = len(self.scanner.deduplicator.active_signals)
                
                log.info(f"📊 Adaptive stats: {signals_found} signals, Active: {active_count}")
                log.info(f"   Fast: {stats['fast_mode_signals']} ({stats.get('fast_mode_ratio', 0):.1%})")
                log.info(f"   Full: {stats['full_mode_signals']} ({stats.get('full_mode_ratio', 0):.1%})")
                log.info(f"   Mode switches: {stats['mode_switches']}")
                
                scan_duration = time.time() - start_time
                log.info(f"Adaptive scan #{self.scan_cycle}: {signals_found} signals in {scan_duration:.2f}s")
                
                if self.scan_cycle % 20 == 0:
                    log.info(f"📈 Detailed adaptive stats: {stats}")
                
                wait_time = max(0.1, SCAN_INTERVAL - scan_duration)
                await asyncio.sleep(wait_time)
                
            except Exception as e:
                log.error(f"Adaptive scanning error: {e}")
                await asyncio.sleep(10)
    
    async def run(self):
        try:
            await self.initialize()
            
            await asyncio.gather(
                self.adaptive_scanning(),
                self.monitor_positions()
            )
            
        except KeyboardInterrupt:
            log.info("Adaptive scanner stopped by user")
            await self.send_final_stats()
            
        except Exception as e:
            log.error(f"Scanner crashed: {e}")
            
        finally:
            await self.cleanup()
    
    async def send_final_stats(self):
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return
        
        try:
            stats = self.scanner.get_daily_stats()
            active_count = len(self.scanner.deduplicator.active_signals)
            
            message = f"""
🛑 <b>تم إيقاف ماسح الرفض المتكيف</b>

<b>📊 إحصائيات اليوم:</b>
• عمليات المسح: {self.scan_cycle}
• الأزواج الممسوحة: {stats['pairs_scanned']}
• إشارات مرسلة: {stats['total_signals']}
• نمط سريع: {stats['fast_mode_signals']} ({stats.get('fast_mode_ratio', 0):.1%})
• نمط كامل: {stats['full_mode_signals']} ({stats.get('full_mode_ratio', 0):.1%})

<b>🔄 التكيف:</b>
• تبديل النمط: {stats['mode_switches']} مرة
• سياق كامل: {stats['choppy_market_full'] + stats['late_wave_full'] + stats['high_vol_full']}

<b>🚫 الفلترة:</b>
• رفض بدون تأكيد: {stats['rejections_no_confirmation']}
• فشل القواعد: {stats['failed_non_negotiables']}

<b>⚡ الصفقات النشطة:</b>
• حالياً: {active_count} صفقة نشطة

<b>🧠 فلسفة النظام:</b>
تم التكيف بنجاح بين:
• السرعة (70% نمط سريع)
• الدقة (30% نمط كامل)
• الحفاظ على القواعد الأساسية

#إحصائيات_متكيفة #توازن_السرعة_والدقة #متداول_مرن
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML"
                })
                
            log.info("✅ Final stats sent to Telegram")
                
        except Exception as e:
            log.error(f"Final stats error: {e}")
    
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

# ================ HTTP SERVER ================
async def start_http_server(scanner, port=8000):
    async def handle_request(reader, writer):
        try:
            request = await reader.read(1024)
            lines = request.decode().split('\r\n')
            
            if not lines:
                writer.write(b'HTTP/1.1 400 Bad Request\r\n\r\n')
                await writer.drain()
                writer.close()
                return
            
            request_line = lines[0]
            method, path, _ = request_line.split(' ')
            
            response = ""
            
            if path == '/':
                stats = scanner.scanner.get_daily_stats()
                active_count = len(scanner.scanner.deduplicator.active_signals)
                
                response = json.dumps({
                    "status": "running",
                    "scanner": "Adaptive Rejection System",
                    "scan_cycle": scanner.scan_cycle,
                    "active_trades": active_count,
                    "daily_stats": stats,
                    "philosophy": "Fast Mode (70%) + Full Mode (30%)",
                    "adaptive_behavior": "Context-based confirmation strictness"
                }, indent=2)
            
            elif path == '/stats':
                response = json.dumps(scanner.scanner.get_daily_stats(), indent=2)
            
            elif path == '/modes':
                response = json.dumps({
                    "fast_mode": {
                        "description": "Fast confirmation - enter at rejection candle",
                        "required_elements": 1,
                        "elements": ["RSI_FAILURE", "VOLUME_WEAKNESS", "EMA_REJECTION"],
                        "target_ratio": 0.7
                    },
                    "full_mode": {
                        "description": "Full confirmation - enter at strength shift",
                        "required_elements": 3,
                        "elements": ["FAILURE_CANDLE", "STRENGTH_SHIFT", "VOLUME_CONFIRMATION"],
                        "target_ratio": 0.3
                    },
                    "non_negotiables": [
                        "No trades in the middle",
                        "No trades without rejection",
                        "No trades against HTF bias",
                        "No trades without volume read"
                    ]
                }, indent=2)
            
            else:
                response = json.dumps({"error": "Endpoint not found"})
            
            writer.write(f'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{response}'.encode())
            await writer.drain()
            writer.close()
            
        except Exception as e:
            error_response = json.dumps({"error": str(e)})
            writer.write(f'HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\n\r\n{error_response}'.encode())
            await writer.drain()
            writer.close()
    
    server = await asyncio.start_server(handle_request, '0.0.0.0', port)
    log.info(f"🌐 HTTP server started on port {port}")
    
    async with server:
        await server.serve_forever()

# ================ MAIN ================
async def main():
    scanner = AdaptiveRejectionScannerSystem()
    http_task = asyncio.create_task(start_http_server(scanner))
    await scanner.run()

if __name__ == "__main__":
    asyncio.run(main())