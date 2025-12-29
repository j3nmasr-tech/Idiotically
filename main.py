#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🔥 PROFESSIONAL REJECTION SCANNER - COMPLETE VERSION
Wave-length awareness + Strength analysis + Rejection entries
OPTIMIZED for 150+ coins but keeping all trader logic
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
from concurrent.futures import ThreadPoolExecutor

# ================ COMPLETE CONFIG ================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DB_PATH = "/app/data/pro_rejection_scanner.db"

# Scanning configuration
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 15))   # 15 seconds - balanced
TOP_N_VOLUME = int(os.getenv("TOP_N_VOLUME", 150))    # All top 150 coins
MIN_VOLUME_USDT = 1000000  # $1M minimum

# Trading parameters
MAX_STOP_LOSS_PCT = 1.0
MIN_TARGET_PCT = 1.5
MAX_TARGET_PCT = 4.0
MIN_RISK_REWARD = 2.0

# Rejection scanning
REJECTION_CONFIG = {
    "rsi_long_zone": (40, 50),
    "rsi_short_zone": (50, 60),
    "ema_distance_threshold": 0.5,
    "min_rejection_strength": 0.6,
}

# Timeframes - Complete set for trader analysis
TIMEFRAMES = {
    "1H": "1h",      # Wave length context
    "15M": "15m",    # Market strength
    "5M": "5m",      # Primary analysis
    "3M": "3m",      # Entry timing
}

# EMA periods
EMA_PERIODS = {
    "fast": 9,
    "medium": 21,
    "slow": 50
}

# RSI settings
RSI_PERIOD = 14
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30

# ================ COMPLETE DATA STRUCTURES ================
@dataclass
class WaveContext:
    """Wave length and maturity - CONTEXT ONLY"""
    wave_length: str           # SHORT, MEDIUM, EXTENDED
    wave_maturity: float       # 0-1 (0=early, 1=exhausted)
    expansion_speed: float     # 0-1 (slow to fast)
    structure_type: str        # IMPULSIVE, CORRECTIVE, COMPRESSION
    context_side: str          # BULLISH_CONTEXT, BEARISH_CONTEXT, NEUTRAL
    
    def __str__(self):
        return f"{self.wave_length} wave ({self.structure_type}) - Maturity: {self.wave_maturity:.0%}"

@dataclass
class MarketStrength:
    """Market strength analysis - CRITICAL for decisions"""
    candle_speed: float        # 0-1 (slow to fast)
    distance_ratio: float      # Distance traveled / time
    ema_angle: float           # EMA slope angle
    volume_participation: float # 0-1 volume participation
    strength_score: float      # 0-1 overall strength
    
    # Interpretation flags
    is_continuation: bool      # Strong move + strong volume
    is_rejection_setup: bool   # Strong move + weak volume
    is_absorption: bool        # Weak move + rising volume
    is_compression: bool       # Flat price + compression
    
    def get_strength_type(self):
        if self.is_continuation: return "CONTINUATION"
        if self.is_rejection_setup: return "REJECTION_SETUP"
        if self.is_absorption: return "ABSORPTION"
        if self.is_compression: return "COMPRESSION"
        return "NEUTRAL"

@dataclass
class RejectionZone:
    """Key rejection area - MANDATORY for entries"""
    zone_type: str             # EMA_SUPPORT, EMA_RESISTANCE, RANGE_LOW, RANGE_HIGH
    price_level: float
    strength: float            # 0-1 rejection strength
    volume_confirmation: bool  # Volume spike at rejection
    rsi_position: str          # IN_ZONE, OVEREXTENDED, NEUTRAL
    is_active: bool
    
    def get_zone_name(self):
        names = {
            "EMA_SUPPORT": "EMA Support",
            "EMA_RESISTANCE": "EMA Resistance",
            "RANGE_LOW": "Range Low",
            "RANGE_HIGH": "Range High",
            "FAILED_BREAKDOWN": "Failed Breakdown",
            "FAILED_BREAKOUT": "Failed Breakout"
        }
        return names.get(self.zone_type, self.zone_type)

@dataclass
class RejectionSignal:
    """Complete rejection-based trade signal"""
    signal_id: str
    symbol: str
    side: str                  # LONG, SHORT
    
    # Price levels
    entry_price: float
    stop_loss: float
    take_profit: float
    
    # Analysis context
    wave_context: WaveContext
    market_strength: MarketStrength
    rejection_zone: RejectionZone
    
    # Entry triggers
    rejection_type: str        # EMA_REJECTION, RANGE_REJECTION, FAILED_BREAKOUT
    trigger_candle: str        # REJECTION_CANDLE, WICK_CONFIRMATION, MOMENTUM_SHIFT
    rsi_at_entry: float
    
    # Metrics
    rejection_strength: float  # 0-1 how strong the rejection is
    risk_reward: float
    expected_move_pct: float
    
    # Timing
    timeframe_used: str
    signal_timestamp: float
    conditions_met: List[str]

# ================ PROFESSIONAL LOGGING ================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)8s | %(name)s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger("pro_rejection_scanner")

# ================ CORE ANALYSIS ENGINE ================
class ProfessionalRejectionScanner:
    """Complete rejection scanner with all trader logic"""
    
    class SignalManager:
        """Manage active signals - one per symbol"""
        def __init__(self):
            self.active_signals = {}  # symbol: signal_id
            self.signal_data = {}     # signal_id: {data}
            
        def can_trade(self, symbol: str) -> bool:
            """Check if we can trade this symbol"""
            if symbol not in self.active_signals:
                return True
            
            signal_id = self.active_signals[symbol]
            if signal_id in self.signal_data:
                status = self.signal_data[signal_id].get("status", "UNKNOWN")
                return status == "CLOSED"
            
            return True
        
        def register_signal(self, signal: RejectionSignal):
            """Register new signal"""
            self.active_signals[signal.symbol] = signal.signal_id
            self.signal_data[signal.signal_id] = {
                "symbol": signal.symbol,
                "side": signal.side,
                "entry": signal.entry_price,
                "status": "PENDING",
                "timestamp": time.time()
            }
        
        def update_status(self, signal_id: str, status: str):
            """Update signal status"""
            if signal_id in self.signal_data:
                self.signal_data[signal_id]["status"] = status
                self.signal_data[signal_id]["timestamp"] = time.time()
                
                if status == "CLOSED":
                    # Remove from active after some time
                    pass
    
    def __init__(self):
        self.stats = {
            "total_scans": 0,
            "coins_scanned": 0,
            "signals_found": 0,
            "long_signals": 0,
            "short_signals": 0,
            "filtered_duplicate": 0,
            "filtered_no_strength": 0,
            "filtered_no_zone": 0
        }
        self.signal_manager = self.SignalManager()
        self.executor = ThreadPoolExecutor(max_workers=10)
    
    # ========== WAVE LENGTH ANALYSIS ==========
    
    def analyze_wave_context(self, df_1h: pd.DataFrame, df_15m: pd.DataFrame) -> WaveContext:
        """
        Analyze wave length and maturity - CONTEXT ONLY
        """
        try:
            if df_1h is None or len(df_1h) < 20:
                return self._default_wave_context()
            
            # 1. Wave length from 1H
            wave_length, maturity = self._analyze_wave_length(df_1h)
            
            # 2. Expansion speed from 15M
            expansion_speed = self._analyze_expansion_speed(df_15m if df_15m is not None else df_1h)
            
            # 3. Structure type
            structure = self._determine_structure(df_15m if df_15m is not None else df_1h)
            
            # 4. Context side
            context_side = self._determine_context_side(df_1h)
            
            return WaveContext(
                wave_length=wave_length,
                wave_maturity=maturity,
                expansion_speed=expansion_speed,
                structure_type=structure,
                context_side=context_side
            )
            
        except Exception as e:
            log.debug(f"Wave context error: {e}")
            return self._default_wave_context()
    
    def _analyze_wave_length(self, df: pd.DataFrame) -> Tuple[str, float]:
        """Analyze wave length and maturity"""
        try:
            if len(df) < 30:
                return "MEDIUM", 0.5
            
            prices = df['close'].values[-30:]
            
            # Total move
            total_move = abs(prices[-1] - prices[0])
            avg_move = np.mean(np.abs(np.diff(prices[-20:])))
            
            if avg_move == 0:
                return "MEDIUM", 0.5
            
            # Length classification
            move_ratio = total_move / avg_move
            
            if move_ratio < 15:
                length = "SHORT"
            elif move_ratio < 30:
                length = "MEDIUM"
            else:
                length = "EXTENDED"
            
            # Maturity based on distance from MA
            ma = np.mean(prices[-20:])
            current = prices[-1]
            distance = abs(current - ma) / ma * 100
            
            # Normalize maturity (0-1)
            maturity = min(distance / 10, 1.0)
            
            return length, maturity
            
        except:
            return "MEDIUM", 0.5
    
    def _analyze_expansion_speed(self, df: pd.DataFrame) -> float:
        """Analyze expansion speed"""
        try:
            if len(df) < 10:
                return 0.5
            
            # Recent candle ranges
            recent = df.iloc[-5:]
            ranges = (recent['high'] - recent['low']) / recent['close'] * 100
            avg_range = np.mean(ranges)
            
            # Normalize: 0.5% = 0.5, 1% = 0.75, 2% = 1.0
            return min(avg_range / 2, 1.0)
            
        except:
            return 0.5
    
    def _determine_structure(self, df: pd.DataFrame) -> str:
        """Determine market structure"""
        try:
            if len(df) < 20:
                return "COMPRESSION"
            
            prices = df['close'].values[-20:]
            highs = df['high'].values[-20:]
            lows = df['low'].values[-20:]
            
            # Price change
            change_pct = abs(prices[-1] - prices[0]) / prices[0] * 100
            
            # Range ratio
            range_pct = (np.max(highs) - np.min(lows)) / prices[0] * 100
            
            if change_pct > 3 and range_pct > 5:
                return "IMPULSIVE"
            elif range_pct < 2:
                return "COMPRESSION"
            else:
                return "CORRECTIVE"
                
        except:
            return "COMPRESSION"
    
    def _determine_context_side(self, df: pd.DataFrame) -> str:
        """Determine context side"""
        try:
            if len(df) < 10:
                return "NEUTRAL"
            
            prices = df['close'].values[-10:]
            slope = np.polyfit(range(len(prices)), prices, 1)[0]
            
            if slope > 0.001:
                return "BULLISH_CONTEXT"
            elif slope < -0.001:
                return "BEARISH_CONTEXT"
            else:
                return "NEUTRAL"
                
        except:
            return "NEUTRAL"
    
    # ========== MARKET STRENGTH ANALYSIS ==========
    
    def analyze_market_strength(self, df: pd.DataFrame) -> MarketStrength:
        """
        Analyze market strength - CRITICAL for decisions
        """
        try:
            if df is None or len(df) < 20:
                return self._default_market_strength()
            
            # 1. Candle speed
            candle_speed = self._calculate_candle_speed(df)
            
            # 2. Distance ratio
            distance_ratio = self._calculate_distance_ratio(df)
            
            # 3. EMA angle
            ema_angle = self._calculate_ema_angle(df)
            
            # 4. Volume participation
            volume_participation = self._calculate_volume_participation(df)
            
            # 5. Overall strength
            strength_score = self._calculate_strength_score(
                candle_speed, distance_ratio, ema_angle, volume_participation
            )
            
            # 6. Interpret patterns
            patterns = self._interpret_strength_patterns(df, candle_speed, volume_participation)
            
            return MarketStrength(
                candle_speed=candle_speed,
                distance_ratio=distance_ratio,
                ema_angle=ema_angle,
                volume_participation=volume_participation,
                strength_score=strength_score,
                **patterns
            )
            
        except Exception as e:
            log.debug(f"Strength analysis error: {e}")
            return self._default_market_strength()
    
    def _calculate_candle_speed(self, df: pd.DataFrame) -> float:
        """Calculate candle speed"""
        try:
            if len(df) < 5:
                return 0.5
            
            recent = df.iloc[-5:]
            ranges = (recent['high'] - recent['low']) / recent['close'] * 100
            return min(np.mean(ranges) / 2, 1.0)
            
        except:
            return 0.5
    
    def _calculate_distance_ratio(self, df: pd.DataFrame) -> float:
        """Calculate distance traveled vs time"""
        try:
            if len(df) < 10:
                return 0.5
            
            prices = df['close'].values[-10:]
            distance_pct = abs(prices[-1] - prices[0]) / prices[0] * 100
            return min(distance_pct / 5, 1.0)
            
        except:
            return 0.5
    
    def _calculate_ema_angle(self, df: pd.DataFrame) -> float:
        """Calculate EMA angle"""
        try:
            if len(df) < 20:
                return 0.0
            
            ema = df['close'].ewm(span=9, adjust=False).mean()
            ema_values = ema.values[-10:]
            
            if len(ema_values) < 5:
                return 0.0
            
            slope = np.polyfit(range(len(ema_values)), ema_values, 1)[0]
            avg = np.mean(ema_values)
            
            if avg > 0:
                return min(abs(slope / avg * 1000), 1.0)
            
            return 0.0
            
        except:
            return 0.0
    
    def _calculate_volume_participation(self, df: pd.DataFrame) -> float:
        """Calculate volume participation"""
        try:
            if len(df) < 20:
                return 0.5
            
            recent_vol = df['volume'].values[-5:].mean()
            avg_vol = df['volume'].values[-20:].mean()
            
            if avg_vol > 0:
                ratio = recent_vol / avg_vol
                if ratio >= 1:
                    return min((ratio - 1) * 2, 1.0)
                else:
                    return max((ratio - 1) * 2, 0.0)
            
            return 0.5
            
        except:
            return 0.5
    
    def _calculate_strength_score(self, speed: float, distance: float, 
                                 angle: float, volume: float) -> float:
        """Calculate overall strength score"""
        weights = [0.2, 0.2, 0.2, 0.4]
        return np.average([speed, distance, angle, volume], weights=weights)
    
    def _interpret_strength_patterns(self, df: pd.DataFrame, speed: float, 
                                    volume: float) -> Dict[str, bool]:
        """Interpret strength patterns"""
        try:
            if len(df) < 10:
                return {
                    "is_continuation": False,
                    "is_rejection_setup": False,
                    "is_absorption": False,
                    "is_compression": False
                }
            
            # Price change
            price_change = (df['close'].iloc[-1] - df['close'].iloc[-5]) / df['close'].iloc[-5] * 100
            
            # Continuation: strong move + strong volume
            is_continuation = speed > 0.7 and volume > 0.7 and abs(price_change) > 1
            
            # Rejection setup: strong move + weak volume
            is_rejection_setup = speed > 0.7 and volume < 0.3 and abs(price_change) > 1
            
            # Absorption: weak move + rising volume
            is_absorption = speed < 0.3 and volume > 0.7
            
            # Compression: low range + low volume
            recent_high = df['high'].values[-5:].max()
            recent_low = df['low'].values[-5:].min()
            range_pct = (recent_high - recent_low) / df['close'].iloc[-1] * 100
            is_compression = range_pct < 1 and volume < 0.5
            
            return {
                "is_continuation": is_continuation,
                "is_rejection_setup": is_rejection_setup,
                "is_absorption": is_absorption,
                "is_compression": is_compression
            }
            
        except:
            return {
                "is_continuation": False,
                "is_rejection_setup": False,
                "is_absorption": False,
                "is_compression": False
            }
    
    # ========== REJECTION ZONE ANALYSIS ==========
    
    def find_rejection_zones(self, df: pd.DataFrame, current_price: float, 
                            rsi_value: float, emas: Dict[str, float]) -> List[RejectionZone]:
        """
        Find key rejection zones - MANDATORY for entries
        """
        zones = []
        
        try:
            if df is None or len(df) < 20:
                return zones
            
            # 1. EMA zones
            zones.extend(self._find_ema_zones(current_price, emas))
            
            # 2. Range zones
            zones.extend(self._find_range_zones(df, current_price))
            
            # 3. Failed break zones
            zones.extend(self._find_failed_break_zones(df, current_price))
            
            # 4. Set RSI position
            for zone in zones:
                zone.rsi_position = self._get_rsi_position(rsi_value, zone.zone_type)
            
            # 5. Check volume confirmation
            for zone in zones:
                zone.volume_confirmation = self._check_volume_confirmation(df, zone.zone_type)
            
            # Return only active zones
            return [z for z in zones if z.is_active]
            
        except Exception as e:
            log.debug(f"Zone finding error: {e}")
            return []
    
    def _find_ema_zones(self, current_price: float, emas: Dict[str, float]) -> List[RejectionZone]:
        """Find EMA rejection zones"""
        zones = []
        
        for name, value in emas.items():
            if value == 0:
                continue
            
            distance_pct = abs(current_price - value) / value * 100
            
            if distance_pct <= REJECTION_CONFIG["ema_distance_threshold"]:
                if current_price > value:
                    zone_type = "EMA_SUPPORT"
                else:
                    zone_type = "EMA_RESISTANCE"
                
                strength = 0.7 if name == "fast" else 0.8 if name == "medium" else 0.9
                
                zones.append(RejectionZone(
                    zone_type=zone_type,
                    price_level=value,
                    strength=strength,
                    volume_confirmation=False,
                    rsi_position="NEUTRAL",
                    is_active=True
                ))
        
        return zones
    
    def _find_range_zones(self, df: pd.DataFrame, current_price: float) -> List[RejectionZone]:
        """Find range high/low zones"""
        zones = []
        
        try:
            if len(df) < 20:
                return zones
            
            recent_high = df['high'].values[-20:].max()
            recent_low = df['low'].values[-20:].min()
            
            # Check range high
            high_dist = abs(current_price - recent_high) / recent_high * 100
            if high_dist <= 0.3:
                zones.append(RejectionZone(
                    zone_type="RANGE_HIGH",
                    price_level=recent_high,
                    strength=0.8,
                    volume_confirmation=False,
                    rsi_position="NEUTRAL",
                    is_active=True
                ))
            
            # Check range low
            low_dist = abs(current_price - recent_low) / recent_low * 100
            if low_dist <= 0.3:
                zones.append(RejectionZone(
                    zone_type="RANGE_LOW",
                    price_level=recent_low,
                    strength=0.8,
                    volume_confirmation=False,
                    rsi_position="NEUTRAL",
                    is_active=True
                ))
            
            return zones
            
        except:
            return []
    
    def _find_failed_break_zones(self, df: pd.DataFrame, current_price: float) -> List[RejectionZone]:
        """Find failed breakout/breakdown zones"""
        zones = []
        
        try:
            if len(df) < 10:
                return zones
            
            # Recent high/low
            recent_high = df['high'].values[-5:].max()
            prev_high = df['high'].values[-10:-5].max()
            
            # Failed breakout
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
            
            # Recent low
            recent_low = df['low'].values[-5:].min()
            prev_low = df['low'].values[-10:-5].min()
            
            # Failed breakdown
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
            
            return zones
            
        except:
            return []
    
    def _get_rsi_position(self, rsi: float, zone_type: str) -> str:
        """Get RSI position for zone"""
        if "SUPPORT" in zone_type or "LOW" in zone_type or "BREAKDOWN" in zone_type:
            if REJECTION_CONFIG["rsi_long_zone"][0] <= rsi <= REJECTION_CONFIG["rsi_long_zone"][1]:
                return "IN_ZONE"
            elif rsi < 30:
                return "OVEREXTENDED"
        elif "RESISTANCE" in zone_type or "HIGH" in zone_type or "BREAKOUT" in zone_type:
            if REJECTION_CONFIG["rsi_short_zone"][0] <= rsi <= REJECTION_CONFIG["rsi_short_zone"][1]:
                return "IN_ZONE"
            elif rsi > 70:
                return "OVEREXTENDED"
        return "NEUTRAL"
    
    def _check_volume_confirmation(self, df: pd.DataFrame, zone_type: str) -> bool:
        """Check volume confirmation at rejection"""
        try:
            if len(df) < 5:
                return False
            
            recent_vol = df['volume'].values[-2:].mean()
            prev_vol = df['volume'].values[-5:-2].mean()
            
            if prev_vol > 0:
                ratio = recent_vol / prev_vol
                if ratio >= 1.5:
                    return True
            
            # For failed breaks, decreasing volume is good
            if "FAILED" in zone_type:
                vol_trend = np.polyfit(range(5), df['volume'].values[-5:], 1)[0]
                if vol_trend < 0:
                    return True
            
            return False
            
        except:
            return False
    
    # ========== INDICATOR CALCULATIONS ==========
    
    def calculate_rsi(self, prices: pd.Series, period: int = RSI_PERIOD) -> pd.Series:
        """Calculate RSI"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))
    
    def calculate_emas(self, df: pd.DataFrame) -> Dict[str, float]:
        """Calculate EMA values"""
        try:
            emas = {}
            for name, period in EMA_PERIODS.items():
                ema = df['close'].ewm(span=period, adjust=False).mean()
                emas[name] = ema.iloc[-1] if len(ema) > 0 else 0
            return emas
        except:
            return {name: 0 for name in EMA_PERIODS.keys()}
    
    # ========== SIGNAL GENERATION ==========
    
    def analyze_pair(self, symbol: str, multi_tf_data: Dict[str, pd.DataFrame]) -> Optional[RejectionSignal]:
        """
        Complete analysis for a single pair
        """
        try:
            # Get timeframe data
            tf_1h = multi_tf_data.get("1H")
            tf_15m = multi_tf_data.get("15M")
            tf_5m = multi_tf_data.get("5M")
            tf_3m = multi_tf_data.get("3M")
            
            # Check data
            if tf_15m is None or tf_3m is None:
                return None
            
            if len(tf_15m) < 20 or len(tf_3m) < 15:
                return None
            
            # 1. Wave context (1H + 15M)
            wave_context = self.analyze_wave_context(tf_1h, tf_15m)
            
            # 2. Market strength (15M)
            market_strength = self.analyze_market_strength(tf_15m)
            
            # CRITICAL: No strength → no trade
            if market_strength.strength_score < 0.4:
                self.stats["filtered_no_strength"] += 1
                return None
            
            # 3. Current analysis on 3M (entry)
            current_price = tf_3m['close'].iloc[-1]
            emas = self.calculate_emas(tf_3m)
            
            # RSI
            rsi_series = self.calculate_rsi(tf_3m['close'])
            current_rsi = rsi_series.iloc[-1] if len(rsi_series) > 0 else 50
            
            # 4. Rejection zones
            rejection_zones = self.find_rejection_zones(tf_3m, current_price, current_rsi, emas)
            
            if not rejection_zones:
                self.stats["filtered_no_zone"] += 1
                return None
            
            # 5. Filter zones with volume confirmation
            valid_zones = [z for z in rejection_zones if z.volume_confirmation]
            
            if not valid_zones:
                return None
            
            # 6. Select best zone
            best_zone = max(valid_zones, key=lambda z: z.strength)
            
            # 7. Determine trade side
            if best_zone.zone_type in ["EMA_SUPPORT", "RANGE_LOW", "FAILED_BREAKDOWN"]:
                side = "LONG"
            elif best_zone.zone_type in ["EMA_RESISTANCE", "RANGE_HIGH", "FAILED_BREAKOUT"]:
                side = "SHORT"
            else:
                return None
            
            # 8. Check RSI position
            if side == "LONG" and best_zone.rsi_position != "IN_ZONE":
                return None
            elif side == "SHORT" and best_zone.rsi_position != "IN_ZONE":
                return None
            
            # 9. Check deduplication
            if not self.signal_manager.can_trade(symbol):
                self.stats["filtered_duplicate"] += 1
                return None
            
            # 10. Calculate entry levels
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
            
            # Risk/Reward
            risk = abs(entry_price - stop_loss)
            reward = abs(take_profit - entry_price)
            
            if risk == 0:
                return None
            
            risk_reward = reward / risk
            
            if risk_reward < MIN_RISK_REWARD:
                return None
            
            # 11. Calculate rejection strength
            rejection_strength = self._calculate_rejection_strength(
                best_zone, market_strength, wave_context, current_rsi
            )
            
            if rejection_strength < REJECTION_CONFIG["min_rejection_strength"]:
                return None
            
            # 12. Determine rejection type
            rejection_type, trigger_candle = self._analyze_rejection_candle(tf_3m, side, best_zone)
            
            if not rejection_type:
                return None
            
            # 13. Conditions met
            conditions = [
                f"WAVE_{wave_context.wave_length}",
                f"STRENGTH_{market_strength.get_strength_type()}",
                f"ZONE_{best_zone.zone_type}",
                f"RSI_{best_zone.rsi_position}",
                f"REJECTION_{rejection_type}"
            ]
            
            # 14. Create signal
            signal_id = hashlib.md5(
                f"{symbol}:{side}:{entry_price}:{time.time()}".encode()
            ).hexdigest()
            
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
                expected_move_pct=target_pct,
                timeframe_used="3M",
                signal_timestamp=time.time(),
                conditions_met=conditions
            )
            
            # 15. Register signal
            self.signal_manager.register_signal(signal)
            
            # 16. Update stats
            self.stats["signals_found"] += 1
            if side == "LONG":
                self.stats["long_signals"] += 1
            else:
                self.stats["short_signals"] += 1
            
            log.info(f"🎯 {symbol} {side} @ {entry_price:.4f}")
            log.info(f"   Zone: {best_zone.get_zone_name()}, Strength: {rejection_strength:.1%}")
            log.info(f"   RSI: {current_rsi:.1f}, R:R: {risk_reward:.1f}:1")
            
            return signal
            
        except Exception as e:
            log.debug(f"Analysis error for {symbol}: {e}")
            return None
    
    def _calculate_rejection_strength(self, zone: RejectionZone, strength: MarketStrength,
                                     wave: WaveContext, rsi: float) -> float:
        """Calculate rejection strength score"""
        factors = []
        weights = []
        
        # Zone strength (30%)
        factors.append(zone.strength)
        weights.append(0.3)
        
        # Market strength (25%)
        factors.append(strength.strength_score)
        weights.append(0.25)
        
        # Wave context (20%)
        wave_score = 0.5
        if wave.structure_type == "CORRECTIVE":
            wave_score = 0.8
        elif wave.structure_type == "COMPRESSION":
            wave_score = 0.7
        
        wave_score *= (1 - wave.wave_maturity * 0.5)
        factors.append(wave_score)
        weights.append(0.2)
        
        # RSI position (25%)
        rsi_score = 0.9 if zone.rsi_position == "IN_ZONE" else 0.3 if zone.rsi_position == "OVEREXTENDED" else 0.5
        factors.append(rsi_score)
        weights.append(0.25)
        
        return np.average(factors, weights=weights)
    
    def _analyze_rejection_candle(self, df: pd.DataFrame, side: str, 
                                 zone: RejectionZone) -> Tuple[Optional[str], Optional[str]]:
        """Analyze rejection candle"""
        try:
            if len(df) < 3:
                return None, None
            
            current = df.iloc[-1]
            prev = df.iloc[-2]
            
            # Wick rejection
            if side == "LONG":
                if current['low'] < zone.price_level and current['close'] > zone.price_level:
                    return "WICK_REJECTION", "SUPPORT_WICK"
            else:
                if current['high'] > zone.price_level and current['close'] < zone.price_level:
                    return "WICK_REJECTION", "RESISTANCE_WICK"
            
            # Momentum shift
            if side == "LONG":
                if (prev['close'] < prev['open'] and 
                    current['close'] > current['open'] and
                    abs(current['close'] - zone.price_level) / zone.price_level < 0.002):
                    return "MOMENTUM_REJECTION", "BULLISH_REVERSAL"
            else:
                if (prev['close'] > prev['open'] and 
                    current['close'] < current['open'] and
                    abs(current['close'] - zone.price_level) / zone.price_level < 0.002):
                    return "MOMENTUM_REJECTION", "BEARISH_REVERSAL"
            
            # Price rejection
            if side == "LONG":
                if current['low'] <= zone.price_level * 1.001 and current['close'] > zone.price_level:
                    return "PRICE_REJECTION", "SUPPORT_HOLD"
            else:
                if current['high'] >= zone.price_level * 0.999 and current['close'] < zone.price_level:
                    return "PRICE_REJECTION", "RESISTANCE_HOLD"
            
            return None, None
            
        except:
            return None, None
    
    def _default_wave_context(self) -> WaveContext:
        return WaveContext(
            wave_length="MEDIUM",
            wave_maturity=0.5,
            expansion_speed=0.5,
            structure_type="COMPRESSION",
            context_side="NEUTRAL"
        )
    
    def _default_market_strength(self) -> MarketStrength:
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
    
    def get_stats(self):
        return self.stats.copy()

# ================ MAIN SCANNER SYSTEM ================
class ProfessionalScanner:
    """Complete scanner system"""
    
    def __init__(self):
        self.scanner = ProfessionalRejectionScanner()
        self.exchange = None
        self.db = None
        self.scan_cycle = 0
        self.semaphore = asyncio.Semaphore(20)  # Concurrent requests
    
    async def initialize(self):
        """Initialize scanner"""
        log.info("=" * 70)
        log.info("🔥 PROFESSIONAL REJECTION SCANNER - COMPLETE VERSION")
        log.info("=" * 70)
        log.info("PHILOSOPHY: Wave length context + Market strength + Rejection entries")
        log.info(f"TARGET: {TOP_N_VOLUME} coins every {SCAN_INTERVAL} seconds")
        log.info("TIME FRAMES: 1H(wave), 15M(strength), 5M/3M(entry)")
        log.info("ANALYSIS: Complete trader logic with all conditions")
        log.info("=" * 70)
        
        await self._init_database()
        await self._init_exchange()
        await self._send_startup_message()
    
    async def _init_database(self):
        """Initialize database"""
        try:
            os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
            self.db = await aiosqlite.connect(DB_PATH)
            
            await self.db.execute("""
            CREATE TABLE IF NOT EXISTS pro_signals (
                id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                entry_price REAL NOT NULL,
                stop_loss REAL NOT NULL,
                take_profit REAL NOT NULL,
                wave_length TEXT NOT NULL,
                wave_maturity REAL NOT NULL,
                structure_type TEXT NOT NULL,
                strength_type TEXT NOT NULL,
                strength_score REAL NOT NULL,
                zone_type TEXT NOT NULL,
                rejection_strength REAL NOT NULL,
                rsi_at_entry REAL NOT NULL,
                risk_reward REAL NOT NULL,
                expected_move REAL NOT NULL,
                conditions_met TEXT,
                status TEXT DEFAULT 'PENDING',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                triggered_at TIMESTAMP,
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
        """Initialize exchange"""
        try:
            self.exchange = ccxt.mexc({
                "enableRateLimit": True,
                "options": {"defaultType": "swap"},
                "timeout": 30000,
                "rateLimit": 100,
            })
            
            ticker = await self.exchange.fetch_ticker("BTC/USDT:USDT")
            log.info(f"✅ MEXC Futures connected. BTC: ${ticker['last']:.2f}")
            
        except Exception as e:
            log.error(f"Exchange error: {e}")
            raise
    
    async def _send_startup_message(self):
        """Send startup message"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return
        
        try:
            message = f"""
🎯 <b>PROFESSIONAL REJECTION SCANNER - COMPLETE</b>

<b>🧠 TRADER PHILOSOPHY:</b>
1️⃣ <b>Wave Length</b> → Context only (no counting)
   • Length: Short/Medium/Extended
   • Maturity: Early to Exhausted
   • Structure: Impulsive/Corrective/Compression

2️⃣ <b>Market Strength</b> → Decision maker
   • Candle speed
   • Distance traveled
   • EMA angle
   • Volume participation
   • Patterns: Continuation/Rejection/Absorption/Compression

3️⃣ <b>Rejection Zones</b> → Mandatory trigger
   • EMA support/resistance
   • Range highs/lows
   • Failed breakouts/breakdowns
   • Volume confirmation required
   • RSI zones: 40-50(LONG), 50-60(SHORT)

<b>⚡ CONFIGURATION:</b>
• Coins: {TOP_N_VOLUME} top by volume
• Scan: Every {SCAN_INTERVAL} seconds
• Timeframes: 1H/15M/5M/3M
• Data: MEXC Futures (accurate)

<b>🎯 ENTRY PHILOSOPHY:</b>
• Enter on first strong rejection candle
• Enter where others hesitate
• Early entries are intentional
• Comfortable with losses
• Hunt expansion, accept losses

الطول الموجي يحدد السياق
القوة والفوليوم يحددان القرار
والرفض هو الزناد

#ProfessionalScanner #RejectionTrader #CompleteAnalysis
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
    
    async def fetch_pair_data(self, symbol: str) -> Dict[str, pd.DataFrame]:
        """Fetch data for a pair"""
        async with self.semaphore:
            try:
                futures_symbol = f"{symbol}:USDT"
                data = {}
                
                # Fetch all timeframes in parallel
                tasks = []
                for tf_name, tf in TIMEFRAMES.items():
                    tasks.append(self._fetch_timeframe(futures_symbol, tf_name, tf))
                
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for tf_name, result in zip(TIMEFRAMES.keys(), results):
                    if isinstance(result, pd.DataFrame) and len(result) >= 10:
                        data[tf_name] = result
                
                return data
                
            except Exception as e:
                log.debug(f"Fetch error {symbol}: {e}")
                return {}
    
    async def _fetch_timeframe(self, symbol: str, tf_name: str, tf: str) -> pd.DataFrame:
        """Fetch single timeframe"""
        try:
            limit = 50 if tf_name in ["1H", "15M"] else 30
            ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe=tf, limit=limit)
            
            if ohlcv and len(ohlcv) >= 10:
                df = pd.DataFrame(
                    ohlcv,
                    columns=["timestamp", "open", "high", "low", "close", "volume"]
                )
                
                for col in ["open", "high", "low", "close", "volume"]:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                
                df = df.dropna()
                return df if len(df) >= 10 else pd.DataFrame()
            
            return pd.DataFrame()
            
        except Exception as e:
            log.debug(f"TF error {symbol} {tf_name}: {e}")
            return pd.DataFrame()
    
    async def get_active_pairs(self) -> List[Tuple[str, float]]:
        """Get top pairs"""
        try:
            markets = await self.exchange.load_markets()
            pairs = []
            
            for symbol, market in markets.items():
                if (market.get('type') == 'swap' and 
                    market.get('settle') == 'USDT' and
                    '/USDT:' in symbol and
                    market.get('active', False)):
                    
                    try:
                        ticker = await self.exchange.fetch_ticker(symbol)
                        volume = ticker.get('quoteVolume', 0)
                        
                        if volume >= MIN_VOLUME_USDT:
                            simple_symbol = symbol.replace(':USDT', '')
                            pairs.append((simple_symbol, volume))
                    
                    except:
                        continue
            
            pairs.sort(key=lambda x: x[1], reverse=True)
            return pairs[:TOP_N_VOLUME]
            
        except Exception as e:
            log.error(f"Pairs error: {e}")
            return [("BTC/USDT", 1000000000), ("ETH/USDT", 500000000)]
    
    async def save_signal(self, signal: RejectionSignal) -> bool:
        """Save signal"""
        try:
            await self.db.execute("""
                INSERT INTO pro_signals (
                    id, symbol, side, entry_price, stop_loss, take_profit,
                    wave_length, wave_maturity, structure_type,
                    strength_type, strength_score,
                    zone_type, rejection_strength, rsi_at_entry,
                    risk_reward, expected_move, conditions_met
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal.signal_id,
                signal.symbol,
                signal.side,
                signal.entry_price,
                signal.stop_loss,
                signal.take_profit,
                signal.wave_context.wave_length,
                signal.wave_context.wave_maturity,
                signal.wave_context.structure_type,
                signal.market_strength.get_strength_type(),
                signal.market_strength.strength_score,
                signal.rejection_zone.zone_type,
                signal.rejection_strength,
                signal.rsi_at_entry,
                signal.risk_reward,
                signal.expected_move_pct,
                json.dumps(signal.conditions_met)
            ))
            
            await self.db.commit()
            log.info(f"✅ Signal saved: {signal.symbol}")
            return True
            
        except Exception as e:
            log.error(f"Save error: {e}")
            return False
    
    async def send_telegram_alert(self, signal: RejectionSignal):
        """Send Telegram alert"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return
        
        try:
            side_emoji = "🟢" if signal.side == "LONG" else "🔴"
            side_text = "شراء" if signal.side == "LONG" else "بيع"
            
            risk_pct = abs(signal.entry_price - signal.stop_loss) / signal.entry_price * 100
            
            message = f"""
{side_emoji} <b>إشارة احترافية - رفض</b> ⚡

<b>{signal.symbol}</b> | {side_text}

<b>📊 السياق الموجي:</b>
• الطول: {signal.wave_context.wave_length}
• الهيكل: {signal.wave_context.structure_type}
• النضج: {signal.wave_context.wave_maturity:.0%}

<b>💪 قوة السوق:</b>
• النوع: {signal.market_strength.get_strength_type()}
• الدرجة: {signal.market_strength.strength_score:.1%}
• الفوليوم: {signal.market_strength.volume_participation:.1%}

<b>🎯 منطقة الرفض:</b>
• النوع: {signal.rejection_zone.get_zone_name()}
• القوة: {signal.rejection_zone.strength:.1%}
• RSI: {signal.rsi_at_entry:.1f} ({signal.rejection_zone.rsi_position})
• التأكيد: {"✅" if signal.rejection_zone.volume_confirmation else "❌"}

<b>⚡ التنفيذ:</b>
• الدخول: <code>{signal.entry_price:.6f}</code>
• وقف الخسارة: <code>{signal.stop_loss:.6f}</code> ({risk_pct:.2f}%)
• الهدف: <code>{signal.take_profit:.6f}</code> ({signal.expected_move_pct:.1f}%)
• نسبة الربح/المخاطرة: {signal.risk_reward:.1f}:1

<b>📈 الجودة:</b>
• قوة الرفض: {signal.rejection_strength:.1%}
• نوع الرفض: {signal.rejection_type}
• الشروط: {len(signal.conditions_met)}

<b>🧠 فلسفة التاجر:</b>
الدخول عند الرفض فقط
القوة والفوليوم يحددان القرار
الطول الموجي يحدد السياق

#{side_text} #رفض_احترافي #{"دعم" if signal.side == "LONG" else "مقاومة"} #سياق_موجي
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML"
                })
                
            log.info(f"📤 Alert sent: {signal.symbol}")
            
        except Exception as e:
            log.error(f"Telegram error: {e}")
    
    async def scan_batch(self, pairs: List[Tuple[str, float]]) -> int:
        """Scan batch of pairs"""
        signals_found = 0
        
        # Process in parallel batches
        batch_size = 10
        for i in range(0, len(pairs), batch_size):
            batch = pairs[i:i + batch_size]
            
            # Create tasks for this batch
            tasks = []
            for symbol, volume in batch:
                task = asyncio.create_task(self._process_single_pair(symbol))
                tasks.append(task)
            
            # Process batch
            try:
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for result in batch_results:
                    if isinstance(result, RejectionSignal):
                        saved = await self.save_signal(result)
                        if saved:
                            await self.send_telegram_alert(result)
                            signals_found += 1
                
                # Small delay between batches
                if i + batch_size < len(pairs):
                    await asyncio.sleep(0.5)
                    
            except Exception as e:
                log.error(f"Batch error: {e}")
                continue
        
        return signals_found
    
    async def _process_single_pair(self, symbol: str) -> Optional[RejectionSignal]:
        """Process single pair"""
        try:
            # Fetch data
            data = await self.fetch_pair_data(symbol)
            
            # Check we have needed timeframes
            if not all(tf in data for tf in ["15M", "3M"]):
                return None
            
            # Analyze in thread pool
            loop = asyncio.get_event_loop()
            signal = await loop.run_in_executor(
                self.scanner.executor,
                lambda: self.scanner.analyze_pair(symbol, data)
            )
            
            return signal
            
        except Exception as e:
            log.debug(f"Process error {symbol}: {e}")
            return None
    
    async def high_freq_scanning(self):
        """Main scanning loop"""
        log.info("🚀 Starting professional scanning...")
        
        while True:
            try:
                self.scan_cycle += 1
                start_time = time.time()
                
                log.info(f"🔄 Professional scan #{self.scan_cycle}")
                
                # Get pairs
                pairs = await self.get_active_pairs()
                
                if not pairs:
                    log.warning("No pairs found")
                    await asyncio.sleep(SCAN_INTERVAL)
                    continue
                
                log.info(f"Scanning {len(pairs)} pairs")
                
                # Scan all pairs
                signals_found = await self.scan_batch(pairs)
                
                # Update stats
                self.scanner.stats["total_scans"] += 1
                self.scanner.stats["coins_scanned"] += len(pairs)
                
                scan_duration = time.time() - start_time
                log.info(f"✅ Scan #{self.scan_cycle}: {signals_found} signals in {scan_duration:.1f}s")
                
                # Log stats
                if self.scan_cycle % 5 == 0:
                    stats = self.scanner.get_stats()
                    log.info(f"📊 Stats: {stats}")
                
                # Wait for next scan
                wait_time = max(1, SCAN_INTERVAL - scan_duration)
                if wait_time > 1:
                    await asyncio.sleep(wait_time)
                else:
                    await asyncio.sleep(1)
                
            except Exception as e:
                log.error(f"Scanning error: {e}")
                await asyncio.sleep(10)
    
    async def monitor_positions(self):
        """Monitor positions"""
        log.info("👀 Starting position monitoring...")
        
        while True:
            try:
                # Get open positions
                async with self.db.execute("""
                    SELECT id, symbol, side, entry_price, stop_loss, take_profit, status
                    FROM pro_signals WHERE status IN ('PENDING', 'TRIGGERED')
                """) as cursor:
                    positions = await cursor.fetchall()
                
                for pos_id, symbol, side, entry, sl, tp, status in positions:
                    try:
                        # Get current price
                        futures_symbol = f"{symbol}:USDT"
                        ticker = await self.exchange.fetch_ticker(futures_symbol)
                        current_price = ticker['last']
                        
                        # Check entry
                        if status == 'PENDING' and abs(current_price - entry) / entry <= 0.005:
                            await self.db.execute("""
                                UPDATE pro_signals SET status = 'TRIGGERED',
                                triggered_at = CURRENT_TIMESTAMP WHERE id = ?
                            """, (pos_id,))
                            await self.db.commit()
                            
                            # Update scanner
                            signal_id = self.scanner.signal_manager.active_signals.get(symbol)
                            if signal_id:
                                self.scanner.signal_manager.update_status(signal_id, "TRIGGERED")
                            
                            log.info(f"✅ Triggered: {symbol}")
                        
                        # Check SL/TP
                        pnl = 0
                        reason = None
                        
                        if side == "LONG":
                            if current_price <= sl:
                                reason = "SL_HIT"
                                pnl = ((current_price - entry) / entry) * 100
                            elif current_price >= tp:
                                reason = "TP_HIT"
                                pnl = ((current_price - entry) / entry) * 100
                        else:
                            if current_price >= sl:
                                reason = "SL_HIT"
                                pnl = ((entry - current_price) / entry) * 100
                            elif current_price <= tp:
                                reason = "TP_HIT"
                                pnl = ((entry - current_price) / entry) * 100
                        
                        if reason:
                            # Update database
                            await self.db.execute("""
                                UPDATE pro_signals SET status = 'CLOSED',
                                closed_at = CURRENT_TIMESTAMP, close_price = ?,
                                pnl_percent = ?, close_reason = ? WHERE id = ?
                            """, (current_price, pnl, reason, pos_id))
                            await self.db.commit()
                            
                            # Update scanner
                            signal_id = self.scanner.signal_manager.active_signals.get(symbol)
                            if signal_id:
                                self.scanner.signal_manager.update_status(signal_id, "CLOSED")
                            
                            log.info(f"📤 Closed: {symbol} {reason} ({pnl:.2f}%)")
                    
                    except Exception as e:
                        log.debug(f"Monitor error {symbol}: {e}")
                
                await asyncio.sleep(2)
                
            except Exception as e:
                log.error(f"Monitor loop error: {e}")
                await asyncio.sleep(5)
    
    async def run(self):
        """Run scanner"""
        try:
            await self.initialize()
            
            await asyncio.gather(
                self.high_freq_scanning(),
                self.monitor_positions()
            )
            
        except KeyboardInterrupt:
            log.info("Scanner stopped")
        except Exception as e:
            log.error(f"Scanner crashed: {e}")
        finally:
            await self.cleanup()
    
    async def cleanup(self):
        """Cleanup"""
        try:
            if self.exchange:
                await self.exchange.close()
            if self.db:
                await self.db.close()
            if self.scanner.executor:
                self.scanner.executor.shutdown(wait=False)
        except:
            pass

# ================ MAIN ================
async def main():
    scanner = ProfessionalScanner()
    await scanner.run()

if __name__ == "__main__":
    asyncio.run(main())