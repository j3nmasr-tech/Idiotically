#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🔥 REJECTION-BASED HIGH-FREQUENCY SCANNER - TRADER'S METHOD
Minimal changes: Only 1, 2, 3, 4, 5 from trader's method
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

# ================ HIGH-FREQUENCY CONFIG ================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DB_PATH = "/app/data/rejection_scanner.db"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 5))
TOP_N_VOLUME = int(os.getenv("TOP_N_VOLUME", 100))
MIN_VOLUME_USD = 500000

# Trading parameters (REJECTION-BASED)
MAX_STOP_LOSS_PCT = 1.0
MIN_TARGET_PCT = 1.5
MAX_TARGET_PCT = 6.0
MIN_RISK_REWARD = 2.0

# Rejection scanning
REJECTION_CONFIG = {
    "rsi_long_zone": (40, 50),
    "rsi_short_zone": (50, 60),
    "ema_distance_threshold": 0.5,
    "min_rejection_strength": 0.6,
}

# Timeframes for REACTION TRADING
TIMEFRAMES = {
    "1H": "1h",
    "15M": "15m",
    "5M": "5m",
    "3M": "3m",
    "1M": "1m"
}

# EMA periods for rejection detection
EMA_PERIODS = {
    "fast": 9,
    "medium": 21,
    "slow": 50
}

# RSI settings for rejection zones
RSI_PERIOD = 14
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30

# ================ DATA STRUCTURES ================
@dataclass
class WaveContext:
    """Wave length and maturity context"""
    wave_length: str
    wave_maturity: float
    expansion_speed: float
    structure_type: str
    context_side: str

@dataclass
class MarketStrength:
    """Market strength analysis"""
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
    """Key rejection area analysis"""
    zone_type: str
    price_level: float
    strength: float
    volume_confirmation: bool
    rsi_position: str
    is_active: bool

@dataclass
class RejectionSignal:
    """Rejection-based trade signal"""
    signal_id: str
    symbol: str
    side: str
    
    entry_price: float
    stop_loss: float
    take_profit: float
    
    wave_context: WaveContext
    market_strength: MarketStrength
    rejection_zone: RejectionZone
    
    rejection_type: str
    trigger_candle: str
    rsi_at_entry: float
    
    rejection_strength: float
    risk_reward: float
    expected_move_pct: float
    
    timeframe_used: str
    signal_timestamp: float
    conditions_met: List[str]

# ================ PROFESSIONAL LOGGING ================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)8s | %(name)s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger("rejection_scanner")

# ================ TRADER'S MINIMAL CHANGES ================
class TraderMethod:
    """Only the 5 things the trader does"""
    
    @staticmethod
    def check_all_timeframes(multi_tf_data: Dict[str, pd.DataFrame]) -> bool:
        """
        1. اراقب كل الفريمات
        Check all timeframes are available and valid
        """
        required = ["1H", "15M", "5M", "3M"]
        for tf in required:
            if tf not in multi_tf_data:
                return False
            if len(multi_tf_data[tf]) < 20:
                return False
        return True
    
    @staticmethod
    def analyze_wave_range_simple(df_1h: pd.DataFrame, df_15m: pd.DataFrame) -> Tuple[str, str]:
        """
        2. احدد المدي الموجي
        Simple wave range analysis
        """
        try:
            if df_1h is None or df_15m is None:
                return "MEDIUM", "NEUTRAL"
            
            # Use 1H for wave range
            if len(df_1h) >= 30:
                prices = df_1h['close'].values[-30:]
                highs = df_1h['high'].values[-30:]
                lows = df_1h['low'].values[-30:]
                
                # Simple range classification
                total_range = max(highs) - min(lows)
                avg_candle = np.mean(df_1h['high'] - df_1h['low'])
                
                if avg_candle == 0:
                    return "MEDIUM", "NEUTRAL"
                
                wave_length_ratio = total_range / avg_candle
                
                if wave_length_ratio < 20:
                    wave_range = "SHORT"
                elif wave_length_ratio < 40:
                    wave_range = "MEDIUM"
                else:
                    wave_range = "LONG"
                
                # Simple phase based on distance from MA
                ma_20 = np.mean(prices[-20:])
                current = prices[-1]
                distance = abs(current - ma_20) / ma_20 * 100
                
                if distance < 5:
                    phase = "EARLY"
                elif distance < 10:
                    phase = "MIDDLE"
                else:
                    phase = "LATE"
                
                return wave_range, phase
            
            return "MEDIUM", "NEUTRAL"
            
        except Exception:
            return "MEDIUM", "NEUTRAL"
    
    @staticmethod
    def check_indicators_trader(df_5m: pd.DataFrame) -> Tuple[float, bool, bool, float]:
        """
        3. RSI AND EMA AND VOL
        Trader's way of checking indicators
        """
        try:
            if len(df_5m) < 20:
                return 50, False, False, 0
            
            # RSI
            rsi = TraderMethod._calculate_rsi_simple(df_5m['close'])
            
            # EMA alignment (9, 21, 50)
            current = df_5m['close'].iloc[-1]
            ema_9 = df_5m['close'].ewm(span=9, adjust=False).mean().iloc[-1]
            ema_21 = df_5m['close'].ewm(span=21, adjust=False).mean().iloc[-1]
            ema_50 = df_5m['close'].ewm(span=50, adjust=False).mean().iloc[-1]
            
            ema_bullish = (current > ema_9 > ema_21 > ema_50)
            ema_bearish = (current < ema_9 < ema_21 < ema_50)
            
            # Volume spike
            recent_vol = df_5m['volume'].values[-5:].mean()
            avg_vol = df_5m['volume'].values[-20:].mean()
            volume_spike = recent_vol > avg_vol * 1.5 if avg_vol > 0 else False
            
            return rsi, ema_bullish, ema_bearish, volume_spike
            
        except Exception:
            return 50, False, False, False
    
    @staticmethod
    def analyze_strength_volume_trader(df_5m: pd.DataFrame) -> Tuple[float, float]:
        """
        4. والقوة والVolume
        Trader's strength and volume analysis
        """
        try:
            if len(df_5m) < 10:
                return 0.5, 0.5
            
            # Strength: based on recent candles
            recent = df_5m.iloc[-5:]
            strength = 0
            
            for _, candle in recent.iterrows():
                body = abs(candle['close'] - candle['open'])
                total = candle['high'] - candle['low']
                
                if total > 0:
                    # Strong candle has small wicks
                    if body / total > 0.7:
                        if candle['close'] > candle['open']:
                            strength += 0.25  # Strong bullish
                        else:
                            strength -= 0.25  # Strong bearish
            
            strength = max(-1, min(1, strength))  # Normalize to -1 to 1
            
            # Volume participation
            recent_vol = df_5m['volume'].values[-5:].mean()
            avg_vol = df_5m['volume'].values[-20:].mean()
            
            if avg_vol > 0:
                vol_ratio = min(recent_vol / avg_vol, 3.0)
                vol_score = (vol_ratio - 1.0) / 2.0  # 1.0 = 0, 3.0 = 1.0
                vol_score = max(0, min(1, vol_score))
            else:
                vol_score = 0.5
            
            return strength, vol_score
            
        except Exception:
            return 0.5, 0.5
    
    @staticmethod
    def determine_direction_trader(df_15m: pd.DataFrame, df_5m: pd.DataFrame) -> str:
        """
        5. واحد الاتجاه
        Trader's direction determination
        """
        try:
            # Primary: 15M trend
            if len(df_15m) >= 20:
                ma_fast_15 = df_15m['close'].rolling(9).mean()
                ma_slow_15 = df_15m['close'].rolling(21).mean()
                
                if len(ma_fast_15) > 1 and len(ma_slow_15) > 1:
                    if ma_fast_15.iloc[-1] > ma_slow_15.iloc[-1] and ma_fast_15.iloc[-2] <= ma_slow_15.iloc[-2]:
                        return "BULLISH_TREND"
                    elif ma_fast_15.iloc[-1] < ma_slow_15.iloc[-1] and ma_fast_15.iloc[-2] >= ma_slow_15.iloc[-2]:
                        return "BEARISH_TREND"
            
            # Secondary: 5M momentum
            if len(df_5m) >= 10:
                prices_5m = df_5m['close'].values[-10:]
                if prices_5m[-1] > np.mean(prices_5m[-5:]):
                    return "BULLISH_MOMENTUM"
                elif prices_5m[-1] < np.mean(prices_5m[-5:]):
                    return "BEARISH_MOMENTUM"
            
            return "NEUTRAL"
            
        except Exception:
            return "NEUTRAL"
    
    @staticmethod
    def _calculate_rsi_simple(prices: pd.Series, period: int = 14) -> float:
        """Simple RSI calculation"""
        try:
            delta = prices.diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            return rsi.iloc[-1] if len(rsi) > 0 else 50
        except:
            return 50

# ================ CORE REJECTION ENGINE ================
class RejectionBasedScanner:
    """High-frequency rejection scanner - MINIMAL CHANGES"""
    
    class SignalDeduplicator:
        """Prevents duplicate signal generation"""
        
        def __init__(self):
            self.active_signals = {}
            self.signal_status = {}
        
        def should_generate_signal(self, symbol: str, side: str, price: float) -> bool:
            if symbol in self.active_signals:
                signal_id = self.active_signals[symbol]
                if signal_id in self.signal_status:
                    status = self.signal_status[signal_id].get("status", "UNKNOWN")
                    if status != "CLOSED":
                        log.debug(f"{symbol}: Active {side} signal exists (status: {status})")
                        return False
            return True
        
        def register_signal(self, signal: RejectionSignal):
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
                "timestamp": signal.signal_timestamp
            }
            
            log.debug(f"Registered rejection signal {signal.signal_id[:8]} for {symbol}")
        
        def update_signal_status(self, signal_id: str, status: str):
            if signal_id in self.signal_status:
                self.signal_status[signal_id]["status"] = status
                log.debug(f"Signal {signal_id[:8]} status updated to {status}")
                
                if status == "CLOSED":
                    symbol = self.signal_status[signal_id]["symbol"]
                    log.info(f"✅ Signal {signal_id[:8]} for {symbol} CLOSED - Ready for new rejections")
        
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
                log.debug(f"Cleaned up closed signal {signal_id[:8]} for {symbol}")
    
    def __init__(self):
        self.daily_stats = {
            "rejections_found": 0,
            "long_rejections": 0,
            "short_rejections": 0,
            "pairs_scanned": 0,
            "rejections_filtered": 0,
            "no_strength": 0,
            "no_rejection_zone": 0,
            "trader_method_failed": 0
        }
        self.deduplicator = self.SignalDeduplicator()
        self.active_signal_ids = set()
    
    # ========== KEEP ORIGINAL WAVE ANALYSIS ==========
    def analyze_wave_context(self, df_1h: pd.DataFrame, df_15m: pd.DataFrame) -> WaveContext:
        """Analyze wave length and maturity"""
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
        """Analyze wave length and maturity"""
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
            
        except Exception as e:
            return "MEDIUM", 0.5
    
    def _analyze_expansion_speed(self, df: pd.DataFrame) -> float:
        """Analyze expansion speed"""
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
            expansion_speed = min(avg_speed / 5.0, 1.0)
            
            return expansion_speed
            
        except Exception as e:
            return 0.5
    
    def _determine_structure(self, df: pd.DataFrame) -> str:
        """Determine market structure type"""
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
                
        except Exception as e:
            return "COMPRESSION"
    
    def _determine_context_side(self, df_1h: pd.DataFrame, df_15m: pd.DataFrame) -> str:
        """Determine context side"""
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
                
        except Exception as e:
            return "NEUTRAL"
    
    def _get_default_wave_context(self) -> WaveContext:
        return WaveContext(
            wave_length="MEDIUM",
            wave_maturity=0.5,
            expansion_speed=0.5,
            structure_type="COMPRESSION",
            context_side="NEUTRAL"
        )
    
    # ========== KEEP ORIGINAL MARKET STRENGTH ==========
    def analyze_market_strength(self, df: pd.DataFrame) -> MarketStrength:
        """Analyze market strength"""
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
        """Calculate average candle speed"""
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
            
        except Exception as e:
            return 0.5
    
    def _calculate_distance_ratio(self, df: pd.DataFrame) -> float:
        """Calculate distance traveled vs time ratio"""
        try:
            if len(df) < 10:
                return 0.5
            
            prices = df['close'].values[-10:]
            total_distance = abs(prices[-1] - prices[0])
            
            if prices[0] > 0:
                distance_pct = total_distance / prices[0] * 100
                return min(distance_pct / 5.0, 1.0)
            
            return 0.5
            
        except Exception as e:
            return 0.5
    
    def _calculate_ema_angle(self, df: pd.DataFrame) -> float:
        """Calculate EMA angle/slope"""
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
            
        except Exception as e:
            return 0.0
    
    def _calculate_volume_participation(self, df: pd.DataFrame) -> float:
        """Calculate volume participation ratio"""
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
            
        except Exception as e:
            return 0.5
    
    def _calculate_strength_score(self, candle_speed: float, distance_ratio: float, 
                                 ema_angle: float, volume_participation: float) -> float:
        """Calculate overall strength score"""
        weights = [0.2, 0.2, 0.2, 0.4]
        factors = [candle_speed, distance_ratio, ema_angle, volume_participation]
        return np.average(factors, weights=weights)
    
    def _interpret_strength_patterns(self, df: pd.DataFrame, candle_speed: float, 
                                    volume_participation: float) -> Tuple[bool, bool, bool, bool]:
        """Interpret strength patterns"""
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
            
        except Exception as e:
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
    
    # ========== KEEP ORIGINAL REJECTION ZONES ==========
    def find_rejection_zones(self, df: pd.DataFrame, current_price: float, 
                            rsi_value: float, emas: Dict[str, float]) -> List[RejectionZone]:
        """Find all active rejection zones"""
        zones = []
        
        try:
            if df is None or len(df) < 20:
                return zones
            
            ema_zones = self._find_ema_rejection_zones(current_price, emas)
            zones.extend(ema_zones)
            
            range_zones = self._find_range_rejection_zones(df, current_price)
            zones.extend(range_zones)
            
            failed_zones = self._find_failed_break_zones(df, current_price)
            zones.extend(failed_zones)
            
            for zone in zones:
                zone.rsi_position = self._analyze_rsi_position(rsi_value, zone.zone_type)
            
            active_zones = [z for z in zones if z.is_active]
            return active_zones
            
        except Exception as e:
            log.error(f"Rejection zone error: {e}")
            return []
    
    def _find_ema_rejection_zones(self, current_price: float, emas: Dict[str, float]) -> List[RejectionZone]:
        """Find EMA rejection zones"""
        zones = []
        
        try:
            for ema_name, ema_value in emas.items():
                if ema_value == 0:
                    continue
                
                distance_pct = abs(current_price - ema_value) / ema_value * 100
                
                if distance_pct <= REJECTION_CONFIG["ema_distance_threshold"]:
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
                        rsi_position="IN_ZONE",
                        is_active=True
                    ))
            
            return zones
            
        except Exception as e:
            return []
    
    def _find_range_rejection_zones(self, df: pd.DataFrame, current_price: float) -> List[RejectionZone]:
        """Find range high/low rejection zones"""
        zones = []
        
        try:
            if len(df) < 20:
                return zones
            
            recent_high = df['high'].values[-20:].max()
            recent_low = df['low'].values[-20:].min()
            
            high_distance_pct = abs(current_price - recent_high) / recent_high * 100
            if high_distance_pct <= 0.3:
                zones.append(RejectionZone(
                    zone_type="RANGE_HIGH",
                    price_level=recent_high,
                    strength=0.8,
                    volume_confirmation=False,
                    rsi_position="IN_ZONE",
                    is_active=True
                ))
            
            low_distance_pct = abs(current_price - recent_low) / recent_low * 100
            if low_distance_pct <= 0.3:
                zones.append(RejectionZone(
                    zone_type="RANGE_LOW",
                    price_level=recent_low,
                    strength=0.8,
                    volume_confirmation=False,
                    rsi_position="IN_ZONE",
                    is_active=True
                ))
            
            return zones
            
        except Exception as e:
            return []
    
    def _find_failed_break_zones(self, df: pd.DataFrame, current_price: float) -> List[RejectionZone]:
        """Find failed breakout/breakdown zones"""
        zones = []
        
        try:
            if len(df) < 10:
                return zones
            
            recent_high = df['high'].values[-5:].max()
            prev_high = df['high'].values[-10:-5].max()
            
            if current_price < recent_high and recent_high > prev_high * 1.005:
                if any(df['close'].values[-5:] < recent_high * 0.995):
                    zones.append(RejectionZone(
                        zone_type="FAILED_BREAKOUT",
                        price_level=recent_high,
                        strength=0.85,
                        volume_confirmation=False,
                        rsi_position="IN_ZONE",
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
                        rsi_position="IN_ZONE",
                        is_active=True
                    ))
            
            return zones
            
        except Exception as e:
            return []
    
    def _analyze_rsi_position(self, rsi_value: float, zone_type: str) -> str:
        """Analyze RSI position relative to zone"""
        if "SUPPORT" in zone_type or "LOW" in zone_type or "BREAKDOWN" in zone_type:
            if REJECTION_CONFIG["rsi_long_zone"][0] <= rsi_value <= REJECTION_CONFIG["rsi_long_zone"][1]:
                return "IN_ZONE"
            elif rsi_value < 30:
                return "OVEREXTENDED"
            else:
                return "NEUTRAL"
        
        elif "RESISTANCE" in zone_type or "HIGH" in zone_type or "BREAKOUT" in zone_type:
            if REJECTION_CONFIG["rsi_short_zone"][0] <= rsi_value <= REJECTION_CONFIG["rsi_short_zone"][1]:
                return "IN_ZONE"
            elif rsi_value > 70:
                return "OVEREXTENDED"
            else:
                return "NEUTRAL"
        
        return "NEUTRAL"
    
    def calculate_rsi(self, prices: pd.Series, period: int = RSI_PERIOD) -> pd.Series:
        """Calculate RSI"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def calculate_emas(self, df: pd.DataFrame) -> Dict[str, float]:
        """Calculate current EMA values"""
        try:
            emas = {}
            for name, period in EMA_PERIODS.items():
                ema_series = df['close'].ewm(span=period, adjust=False).mean()
                emas[name] = ema_series.iloc[-1] if len(ema_series) > 0 else 0
            return emas
        except Exception as e:
            return {name: 0 for name in EMA_PERIODS.keys()}
    
    # ========== MODIFIED: TRADER'S METHOD INTEGRATION ==========
    def generate_rejection_signal(self, multi_tf_data: Dict[str, pd.DataFrame], 
                                 symbol: str) -> Optional[RejectionSignal]:
        """
        Generate rejection-based signal WITH TRADER'S 5 METHODS
        """
        try:
            # Get timeframe data
            tf_1h = multi_tf_data.get("1H")
            tf_15m = multi_tf_data.get("15M")
            tf_5m = multi_tf_data.get("5M")
            tf_3m = multi_tf_data.get("3M")
            tf_1m = multi_tf_data.get("1M")
            
            # 1. اراقب كل الفريمات - Check all timeframes
            if not TraderMethod.check_all_timeframes(multi_tf_data):
                log.debug(f"{symbol}: Missing timeframes")
                return None
            
            # 2. احدد المدي الموجي - Analyze wave range
            wave_range, wave_phase = TraderMethod.analyze_wave_range_simple(tf_1h, tf_15m)
            
            # Skip late phases (exhausted moves)
            if wave_phase == "LATE":
                log.debug(f"{symbol}: Wave phase LATE (exhausted)")
                self.daily_stats["trader_method_failed"] += 1
                return None
            
            # 3. RSI AND EMA AND VOL - Check indicators
            rsi_value, ema_bullish, ema_bearish, volume_spike = TraderMethod.check_indicators_trader(tf_5m)
            
            # Need clear EMA alignment
            if not (ema_bullish or ema_bearish):
                log.debug(f"{symbol}: No clear EMA alignment")
                self.daily_stats["trader_method_failed"] += 1
                return None
            
            # 4. والقوة والVolume - Analyze strength and volume
            strength, volume_score = TraderMethod.analyze_strength_volume_trader(tf_5m)
            
            # Need decent strength and volume
            if strength < 0.2 and volume_score < 0.3:
                log.debug(f"{symbol}: Weak strength ({strength:.2f}) and volume ({volume_score:.2f})")
                self.daily_stats["trader_method_failed"] += 1
                return None
            
            # 5. واحد الاتجاه - Determine direction
            direction = TraderMethod.determine_direction_trader(tf_15m, tf_5m)
            
            if direction == "NEUTRAL":
                log.debug(f"{symbol}: No clear direction")
                self.daily_stats["trader_method_failed"] += 1
                return None
            
            # ========== ORIGINAL REJECTION ANALYSIS ==========
            # Continue with your original analysis...
            wave_context = self.analyze_wave_context(tf_1h, tf_15m)
            market_strength = self.analyze_market_strength(tf_15m)
            
            if market_strength.strength_score < 0.4:
                self.daily_stats["no_strength"] += 1
                log.debug(f"{symbol}: No market strength ({market_strength.strength_score:.2f})")
                return None
            
            current_price = tf_3m['close'].iloc[-1]
            emas = self.calculate_emas(tf_3m)
            
            rsi_series = self.calculate_rsi(tf_3m['close'])
            current_rsi = rsi_series.iloc[-1] if len(rsi_series) > 0 else 50
            
            rejection_zones = self.find_rejection_zones(tf_3m, current_price, current_rsi, emas)
            
            if not rejection_zones:
                self.daily_stats["no_rejection_zone"] += 1
                log.debug(f"{symbol}: No active rejection zone")
                return None
            
            valid_zones = []
            for zone in rejection_zones:
                zone.volume_confirmation = self._check_volume_confirmation(tf_3m, zone.zone_type)
                if zone.volume_confirmation:
                    valid_zones.append(zone)
            
            if not valid_zones:
                log.debug(f"{symbol}: No volume confirmation at rejection zones")
                return None
            
            best_zone = max(valid_zones, key=lambda z: z.strength)
            
            side = None
            if best_zone.zone_type in ["EMA_SUPPORT", "RANGE_LOW", "FAILED_BREAKDOWN", "DEMAND"]:
                side = "LONG"
            elif best_zone.zone_type in ["EMA_RESISTANCE", "RANGE_HIGH", "FAILED_BREAKOUT", "SUPPLY"]:
                side = "SHORT"
            
            if not side:
                log.debug(f"{symbol}: Could not determine side for zone {best_zone.zone_type}")
                return None
            
            # Check if direction matches side
            if (side == "LONG" and "BEARISH" in direction) or (side == "SHORT" and "BULLISH" in direction):
                log.debug(f"{symbol}: Side-direction mismatch: {side} vs {direction}")
                return None
            
            # DEDUPLICATION CHECK
            if not self.deduplicator.should_generate_signal(symbol, side, current_price):
                self.daily_stats["rejections_filtered"] += 1
                log.debug(f"{symbol}: Already active trade for this symbol")
                return None
            
            rejection_type, trigger_candle = self._analyze_rejection_candle(tf_3m, side, best_zone)
            
            if not rejection_type:
                log.debug(f"{symbol}: No clear rejection candle")
                return None
            
            # Calculate entry, SL, TP
            stop_loss_pct = np.random.uniform(0.5, MAX_STOP_LOSS_PCT)
            target_pct = np.random.uniform(MIN_TARGET_PCT, MAX_TARGET_PCT)
            
            if side == "LONG":
                entry_price = best_zone.price_level * 1.001
                stop_loss = entry_price * (1 - stop_loss_pct / 100)
                take_profit = entry_price * (1 + target_pct / 100)
            else:
                entry_price = best_zone.price_level * 0.999
                stop_loss = entry_price * (1 + stop_loss_pct / 100)
                take_profit = entry_price * (1 - target_pct / 100)
            
            risk = abs(entry_price - stop_loss)
            reward = abs(take_profit - entry_price)
            
            if risk == 0:
                return None
            
            risk_reward = reward / risk
            
            if risk_reward < MIN_RISK_REWARD:
                log.debug(f"{symbol}: R:R too low ({risk_reward:.1f}:1)")
                return None
            
            rejection_strength = self._calculate_rejection_strength(
                best_zone, market_strength, wave_context, current_rsi
            )
            
            if rejection_strength < REJECTION_CONFIG["min_rejection_strength"]:
                log.debug(f"{symbol}: Rejection too weak ({rejection_strength:.2f})")
                return None
            
            # Add trader method results to conditions
            conditions_met = self._get_rejection_conditions(
                wave_context, market_strength, best_zone, rejection_type
            )
            conditions_met.append(f"TRADER_WAVE_{wave_range}_{wave_phase}")
            conditions_met.append(f"TRADER_RSI_{rsi_value:.1f}")
            conditions_met.append(f"TRADER_DIRECTION_{direction}")
            conditions_met.append(f"TRADER_STRENGTH_{strength:.2f}")
            conditions_met.append(f"TRADER_VOLUME_{volume_score:.2f}")
            
            signal_id = hashlib.md5(
                f"{symbol}:{side}:{entry_price:.8f}:{time.time()}:{best_zone.zone_type}".encode()
            ).hexdigest()
            
            # Calculate expected move (3% based on trader method)
            expected_move_pct = 3.0  # Trader expects 3% moves
            
            signal = RejectionSignal(
                signal_id=signal_id,
                symbol=symbol,
                side=side,
                
                entry_price=entry_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
                
                wave_context=wave_context,
                market_strength=market_strength,
                rejection_zone=best_zone,
                
                rejection_type=rejection_type,
                trigger_candle=trigger_candle,
                rsi_at_entry=current_rsi,
                
                rejection_strength=rejection_strength,
                risk_reward=risk_reward,
                expected_move_pct=expected_move_pct,
                
                timeframe_used="3M",
                signal_timestamp=time.time(),
                conditions_met=conditions_met
            )
            
            self.deduplicator.register_signal(signal)
            self.active_signal_ids.add(signal_id)
            
            self.daily_stats["rejections_found"] += 1
            if side == "LONG":
                self.daily_stats["long_rejections"] += 1
            else:
                self.daily_stats["short_rejections"] += 1
            
            log.info(f"🎯 TRADER REJECTION: {symbol} {side} @ {entry_price:.4f}")
            log.info(f"   Wave: {wave_range}/{wave_phase}, RSI: {rsi_value:.1f}")
            log.info(f"   Direction: {direction}, Strength: {strength:.2f}")
            log.info(f"   Zone: {best_zone.zone_type}, R:R: {risk_reward:.1f}:1")
            log.info(f"   Expected: {expected_move_pct:.1f}% move")
            
            return signal
            
        except Exception as e:
            log.error(f"Rejection signal error for {symbol}: {e}")
            return None
    
    def _check_volume_confirmation(self, df: pd.DataFrame, zone_type: str) -> bool:
        """Check volume confirmation at rejection zone"""
        try:
            if len(df) < 5:
                return False
            
            recent_candles = df.iloc[-5:]
            recent_volume = recent_candles['volume'].values[-2:].mean()
            prev_volume = recent_candles['volume'].values[-5:-2].mean()
            
            if prev_volume > 0:
                volume_ratio = recent_volume / prev_volume
                if volume_ratio >= 1.5:
                    return True
            
            if "FAILED" in zone_type:
                volume_trend = np.polyfit(range(5), recent_candles['volume'].values[-5:], 1)[0]
                if volume_trend < 0:
                    return True
            
            return False
            
        except Exception as e:
            return False
    
    def _analyze_rejection_candle(self, df: pd.DataFrame, side: str, zone: RejectionZone) -> Tuple[Optional[str], Optional[str]]:
        """Analyze the rejection candle pattern"""
        try:
            if len(df) < 3:
                return None, None
            
            current_candle = df.iloc[-1]
            prev_candle = df.iloc[-2]
            
            has_wick_rejection = False
            wick_type = None
            
            if side == "LONG":
                if (current_candle['low'] < zone.price_level and 
                    current_candle['close'] > zone.price_level):
                    has_wick_rejection = True
                    wick_type = "SUPPORT_WICK"
            
            else:
                if (current_candle['high'] > zone.price_level and 
                    current_candle['close'] < zone.price_level):
                    has_wick_rejection = True
                    wick_type = "RESISTANCE_WICK"
            
            momentum_shift = False
            candle_type = None
            
            if side == "LONG":
                if (prev_candle['close'] < prev_candle['open'] and
                    current_candle['close'] > current_candle['open'] and
                    abs(current_candle['close'] - zone.price_level) / zone.price_level < 0.002):
                    momentum_shift = True
                    candle_type = "BULLISH_REVERSAL"
            
            else:
                if (prev_candle['close'] > prev_candle['open'] and
                    current_candle['close'] < current_candle['open'] and
                    abs(current_candle['close'] - zone.price_level) / zone.price_level < 0.002):
                    momentum_shift = True
                    candle_type = "BEARISH_REVERSAL"
            
            if has_wick_rejection:
                return "WICK_REJECTION", wick_type
            elif momentum_shift:
                return "MOMENTUM_REJECTION", candle_type
            else:
                if side == "LONG":
                    if current_candle['low'] <= zone.price_level * 1.001 and current_candle['close'] > zone.price_level:
                        return "PRICE_REJECTION", "SUPPORT_HOLD"
                else:
                    if current_candle['high'] >= zone.price_level * 0.999 and current_candle['close'] < zone.price_level:
                        return "PRICE_REJECTION", "RESISTANCE_HOLD"
            
            return None, None
            
        except Exception as e:
            return None, None
    
    def _calculate_rejection_strength(self, zone: RejectionZone, strength: MarketStrength, 
                                     wave: WaveContext, rsi: float) -> float:
        """Calculate overall rejection strength score"""
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
    
    def _get_rejection_conditions(self, wave: WaveContext, strength: MarketStrength, 
                                 zone: RejectionZone, rejection_type: str) -> List[str]:
        """Get list of conditions met for this rejection"""
        conditions = []
        
        conditions.append(f"WAVE_{wave.wave_length}")
        conditions.append(f"STRUCTURE_{wave.structure_type}")
        
        if strength.is_continuation:
            conditions.append("STRENGTH_CONTINUATION")
        if strength.is_rejection_setup:
            conditions.append("STRENGTH_REJECTION_SETUP")
        if strength.is_absorption:
            conditions.append("STRENGTH_ABSORPTION")
        if strength.is_compression:
            conditions.append("STRENGTH_COMPRESSION")
        
        conditions.append(f"ZONE_{zone.zone_type}")
        if zone.volume_confirmation:
            conditions.append("VOLUME_CONFIRMED")
        
        conditions.append(f"REJECTION_{rejection_type}")
        conditions.append(f"RSI_{zone.rsi_position}")
        
        return conditions
    
    def get_daily_stats(self) -> Dict:
        """Get daily statistics"""
        return self.daily_stats
    
    def cleanup_old_signals(self):
        """Clean up old signals from deduplication"""
        self.deduplicator.remove_closed_signals()

# ================ MAIN SCANNER SYSTEM ================
class RejectionScanner:
    """Main scanner system - MINIMAL CHANGES"""
    
    def __init__(self):
        self.scanner = RejectionBasedScanner()
        self.exchange = None
        self.db = None
        self.scan_cycle = 0
    
    # ========== KEEP ALL ORIGINAL METHODS ==========
    async def initialize(self):
        """Initialize the scanner"""
        log.info("=" * 60)
        log.info("🎯 REJECTION SCANNER WITH TRADER'S 5 METHODS")
        log.info("=" * 60)
        log.info("1. اراقب كل الفريمات")
        log.info("2. احدد المدي الموجي")  
        log.info("3. RSI AND EMA AND VOL")
        log.info("4. والقوة والVolume")
        log.info("5. احدد الاتجاه")
        log.info("=" * 60)
        
        await self._init_database()
        await self._init_exchange()
        await self._send_startup_message()
    
    async def _init_database(self):
        """Initialize database"""
        try:
            os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
            self.db = await aiosqlite.connect(DB_PATH)
            
            await self.db.execute("""
            CREATE TABLE IF NOT EXISTS rejection_signals (
                id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                entry_price REAL NOT NULL,
                stop_loss REAL NOT NULL,
                take_profit REAL NOT NULL,
                
                wave_length TEXT NOT NULL,
                wave_maturity REAL NOT NULL,
                expansion_speed REAL NOT NULL,
                structure_type TEXT NOT NULL,
                
                candle_speed REAL NOT NULL,
                distance_ratio REAL NOT NULL,
                ema_angle REAL NOT NULL,
                volume_participation REAL NOT NULL,
                strength_score REAL NOT NULL,
                
                zone_type TEXT NOT NULL,
                rejection_strength REAL NOT NULL,
                rsi_at_entry REAL NOT NULL,
                rejection_type TEXT NOT NULL,
                trigger_candle TEXT NOT NULL,
                
                risk_reward REAL NOT NULL,
                expected_move REAL NOT NULL,
                timeframe_used TEXT NOT NULL,
                
                conditions_met TEXT,
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
            log.info("✅ Database initialized")
            
        except Exception as e:
            log.error(f"Database error: {e}")
            raise
    
    async def _init_exchange(self):
        """Initialize exchange connection"""
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
        """Send startup message to Telegram"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            log.warning("⚠️ Telegram credentials not set")
            return
        
        try:
            message = f"""
🎯 <b>ماسح الرفض مع طريقة المتداول</b>

✅ <b>طريقة المتداول المضافة:</b>
1. اراقب كل الفريمات
2. احدد المدي الموجي  
3. RSI AND EMA AND VOL
4. والقوة والVolume
5. احدد الاتجاه

⚡ <b>الباقي كما هو:</b>
• نفس كمية المسح: {TOP_N_VOLUME} عملة
• نفس التوقيت: كل {SCAN_INTERVAL} ثواني
• نفس نظام الرفض
• نفس إدارة المخاطرة

#ماسح_رفض #طريقة_متداول
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
        """Fetch data for all timeframes"""
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
        """Get active trading pairs"""
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
    
    async def save_signal(self, signal: RejectionSignal) -> bool:
        """Save signal to database"""
        try:
            await self.db.execute("""
                INSERT INTO rejection_signals (
                    id, symbol, side, entry_price, stop_loss, take_profit,
                    wave_length, wave_maturity, expansion_speed, structure_type,
                    candle_speed, distance_ratio, ema_angle, volume_participation, strength_score,
                    zone_type, rejection_strength, rsi_at_entry, rejection_type, trigger_candle,
                    risk_reward, expected_move, timeframe_used,
                    conditions_met
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal.signal_id,
                signal.symbol,
                signal.side,
                signal.entry_price,
                signal.stop_loss,
                signal.take_profit,
                signal.wave_context.wave_length,
                signal.wave_context.wave_maturity,
                signal.wave_context.expansion_speed,
                signal.wave_context.structure_type,
                signal.market_strength.candle_speed,
                signal.market_strength.distance_ratio,
                signal.market_strength.ema_angle,
                signal.market_strength.volume_participation,
                signal.market_strength.strength_score,
                signal.rejection_zone.zone_type,
                signal.rejection_strength,
                signal.rsi_at_entry,
                signal.rejection_type,
                signal.trigger_candle,
                signal.risk_reward,
                signal.expected_move_pct,
                signal.timeframe_used,
                json.dumps(signal.conditions_met)
            ))
            
            await self.db.commit()
            log.info(f"✅ Signal saved: {signal.symbol}")
            return True
            
        except Exception as e:
            log.error(f"Error saving signal: {e}")
            return False
    
    async def format_signal_message(self, signal: RejectionSignal) -> str:
        """Format signal for Telegram"""
        side_emoji = "🟢" if signal.side == "LONG" else "🔴"
        side_text = "شراء" if signal.side == "LONG" else "بيع"
        
        # Extract trader method results
        trader_info = []
        for condition in signal.conditions_met:
            if "TRADER_" in condition:
                trader_info.append(condition.replace("TRADER_", ""))
        
        risk_pct = abs(signal.entry_price - signal.stop_loss) / signal.entry_price * 100
        
        message = f"""
{side_emoji} <b>إشارة رفض بطريقة المتداول</b>

<b>{signal.symbol}</b> | {side_text}

<b>✅ طريقة المتداول:</b>
"""
        
        for info in trader_info[:5]:
            message += f"• {info}\n"
        
        message += f"""
<b>📊 تفاصيل الصفقة:</b>
• سعر الدخول: <code>{signal.entry_price:.6f}</code>
• وقف الخسارة: <code>{signal.stop_loss:.6f}</code> ({risk_pct:.2f}%)
• هدف الربح: <code>{signal.take_profit:.6f}</code> ({signal.expected_move_pct:.1f}%)
• نسبة الربح/المخاطرة: {signal.risk_reward:.1f}:1

<b>🎯 توقع المتداول:</b>
حركة متوقعة: {signal.expected_move_pct:.1f}%

#{side_text} #طريقة_متداول #رفض
"""
        return message
    
    async def send_trade_trigger_notification(self, symbol: str, side: str, entry_price: float):
        """Send notification when trade is triggered"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return
        
        try:
            side_emoji = "🟢" if side == "LONG" else "🔴"
            side_text = "شراء" if side == "LONG" else "بيع"
            
            message = f"""
{side_emoji} <b>تم تنفيذ صفقة طريقة المتداول</b>

<b>{symbol}</b> | {side_text}

🎯 تم الدخول عند: <code>{entry_price:.6f}</code>

#{side_text} #تنفيذ
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML"
                })
            
        except Exception as e:
            log.error(f"Trigger notification error: {e}")
    
    async def send_trade_close_notification(self, symbol: str, side: str, pnl_percent: float, 
                                           close_reason: str, entry_price: float, 
                                           close_price: float, risk_reward: float):
        """Send notification when trade hits TP/SL"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return
        
        try:
            if close_reason == "TP_HIT":
                emoji = "✅"
                result_text = "هدف الربح"
            else:
                emoji = "❌"
                result_text = "وقف الخسارة"
            
            side_text = "شراء" if side == "LONG" else "بيع"
            pnl_formatted = f"+{pnl_percent:.2f}%" if pnl_percent > 0 else f"{pnl_percent:.2f}%"
            
            message = f"""
{emoji} <b>تم إغلاق صفقة طريقة المتداول</b>

<b>{symbol}</b> | {side_text}

📊 النتيجة: {result_text}
💰 النسبة: {pnl_formatted}

#{side_text} #إغلاق #{"ربح" if close_reason == "TP_HIT" else "خسارة"}
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML"
                })
            
        except Exception as e:
            log.error(f"Close notification error: {e}")
    
    async def send_telegram_alert(self, signal: RejectionSignal):
        """Send Telegram alert"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
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
                
            log.info(f"📤 Telegram alert sent: {signal.symbol}")
            
        except Exception as e:
            log.error(f"Telegram error: {e}")
    
    async def monitor_positions(self):
        """Monitor and close positions"""
        log.info("👀 Starting position monitoring...")
        
        while True:
            try:
                async with self.db.execute("""
                    SELECT id, symbol, side, entry_price, stop_loss, take_profit, status
                    FROM rejection_signals 
                    WHERE status IN ('PENDING', 'TRIGGERED')
                """) as cursor:
                    positions = await cursor.fetchall()
                
                for pos_id, symbol, side, entry, sl, tp, status in positions:
                    try:
                        ticker = await self.exchange.fetch_ticker(symbol)
                        current_price = ticker['last']
                        
                        if status == 'PENDING':
                            if abs(current_price - entry) / entry <= 0.005:
                                await self.db.execute("""
                                    UPDATE rejection_signals SET 
                                        status = 'TRIGGERED',
                                        triggered_at = CURRENT_TIMESTAMP,
                                        trigger_price = ?
                                    WHERE id = ?
                                """, (current_price, pos_id))
                                
                                await self.db.commit()
                                self.scanner.deduplicator.update_signal_status(pos_id, "TRIGGERED")
                                await self.send_trade_trigger_notification(symbol, side, current_price)
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
                            async with self.db.execute("""
                                SELECT risk_reward FROM rejection_signals WHERE id = ?
                            """, (pos_id,)) as cursor:
                                row = await cursor.fetchone()
                                risk_reward = row[0] if row else 0
                            
                            await self.db.execute("""
                                UPDATE rejection_signals SET 
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
                                risk_reward=risk_reward
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
    
    async def high_freq_scanning(self):
        """Main high-frequency scanning loop"""
        log.info("🚀 Starting rejection scanning with trader method...")
        
        while True:
            try:
                self.scan_cycle += 1
                start_time = time.time()
                
                log.info(f"🔄 Scan cycle #{self.scan_cycle}")
                
                pairs = await self.get_active_pairs()
                
                if not pairs:
                    await asyncio.sleep(SCAN_INTERVAL)
                    continue
                
                log.info(f"Scanning {len(pairs)} pairs")
                
                signals_found = 0
                
                for symbol, volume in pairs:
                    try:
                        multi_tf_data = await self.fetch_timeframe_data(symbol)
                        signal = self.scanner.generate_rejection_signal(multi_tf_data, symbol)
                        
                        if signal:
                            saved = await self.save_signal(signal)
                            if saved:
                                await self.send_telegram_alert(signal)
                                signals_found += 1
                        
                        await asyncio.sleep(0.01)
                        
                    except Exception as e:
                        log.debug(f"Pair error {symbol}: {str(e)[:50]}")
                        continue
                
                self.scanner.daily_stats["pairs_scanned"] += len(pairs)
                
                stats = self.scanner.get_daily_stats()
                active_count = len(self.scanner.deduplicator.active_signals)
                
                log.info(f"📊 Stats: {signals_found} signals, Active: {active_count}")
                log.info(f"   Trader method failed: {stats.get('trader_method_failed', 0)}")
                
                scan_duration = time.time() - start_time
                wait_time = max(0.1, SCAN_INTERVAL - scan_duration)
                await asyncio.sleep(wait_time)
                
            except Exception as e:
                log.error(f"Scanning loop error: {e}")
                await asyncio.sleep(10)
    
    async def run(self):
        """Run the scanner"""
        try:
            await self.initialize()
            await asyncio.gather(
                self.high_freq_scanning(),
                self.monitor_positions()
            )
            
        except KeyboardInterrupt:
            log.info("Scanner stopped by user")
            
        except Exception as e:
            log.error(f"Scanner crashed: {e}")
            
        finally:
            await self.cleanup()
    
    async def cleanup(self):
        """Cleanup resources"""
        try:
            if self.exchange:
                await self.exchange.close()
            if self.db:
                await self.db.close()
                
        except Exception as e:
            log.error(f"Cleanup error: {e}")

# ================ MAIN ================
async def main():
    """Main function"""
    scanner = RejectionScanner()
    await scanner.run()

if __name__ == "__main__":
    asyncio.run(main())