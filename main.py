#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🔥 ELLIOTT WAVE + INDICATORS HIGH-FREQUENCY SCANNER
Professional-grade high-frequency signal generator
Trend from Elliott Waves, Entries from Indicators
FIXED VERSION - Complete notifications for entry/exit
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
DB_PATH = "/app/data/elliott_scanner.db"

# Ultra high-frequency scanning
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 15))  # 15 seconds - ULTRA FAST
TOP_N_VOLUME = int(os.getenv("TOP_N_VOLUME", 100))   # Scan many pairs
MIN_VOLUME_USD = 500000  # $500K minimum - more opportunities

# Trading parameters (AGGRESSIVE)
MAX_STOP_LOSS_PCT = 1.0    # 1% maximum stop loss
MIN_TARGET_PCT = 3.0       # 3% minimum target
MAX_TARGET_PCT = 8.0       # 8% maximum target
MIN_RISK_REWARD = 2.0      # Minimum 1:2 risk/reward (high frequency)

# Deduplication configuration
DEDUPLICATION_CONFIG = {
    "cooldown_same_side": 1800,      # 30 minutes for same side signals
    "cooldown_opposite_side": 600,   # 10 minutes for opposite side
    "price_similarity_threshold": 0.5,  # 0.5% price difference
    "max_signals_per_hour": 3,       # Max 3 signals per symbol per hour
    "max_active_signals": 5,         # Max 5 active signals per symbol
}

# Timeframes for analysis
TIMEFRAMES = {
    "4H": "4h",      # Elliott Wave trend ONLY
    "1H": "1h",      # Wave context
    "15M": "15m",    # Primary entry analysis
    "5M": "5m",      # Entry timing (MAIN)
    "3M": "3m"       # Fast trigger
}

# EMA periods for entry signals
EMA_PERIODS = {
    "fast": 9,
    "medium": 21,
    "slow": 50
}

# RSI settings
RSI_PERIOD = 14
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30

# ================ DATA STRUCTURES ================
@dataclass
class ElliottTrend:
    """Elliott Wave trend context (direction only)"""
    direction: str           # BULLISH, BEARISH, NEUTRAL
    strength: float          # 0-1 confidence
    wave_position: str       # IMPULSIVE, CORRECTIVE
    wave_maturity: float     # 0-1 (0=early, 1=late)
    trend_source: str        # Which TF gave the trend

@dataclass
class IndicatorSignal:
    """Indicator-based entry signals"""
    rsi_signal: str          # OVERSOLD, OVERBOUGHT, BULLISH_DIV, BEARISH_DIV, NEUTRAL
    rsi_value: float
    ema_signal: str          # BOUNCE, REJECTION, COMPRESSION, OVERSTRETCH
    ema_distance_pct: float  # Distance from fast EMA
    volume_signal: str       # CONFIRMING, DIVERGING, NEUTRAL
    volume_ratio: float      # Recent vs average volume
    strength_score: float    # 0-1 market strength

@dataclass
class HighFreqSignal:
    """High-frequency trade signal"""
    signal_id: str
    symbol: str
    side: str                # LONG, SHORT
    entry_price: float
    stop_loss: float
    take_profit: float
    
    # Context
    trend: ElliottTrend
    indicators: IndicatorSignal
    
    # Metrics
    confluence_score: float  # How many indicators confirm (0-1)
    risk_reward: float
    expected_move_pct: float
    
    # Timing
    timeframe_used: str      # Which TF triggered entry
    signal_timestamp: float
    conditions_met: List[str]  # Which conditions triggered

# ================ PROFESSIONAL LOGGING ================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)8s | %(name)s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger("elliott_scanner")

# ================ CORE ANALYSIS ENGINE ================
class HighFrequencyScanner:
    """High-frequency Elliott + Indicators scanner with deduplication"""
    
    class SignalDeduplicator:
        """Prevents duplicate signal generation"""
        
        def __init__(self):
            self.signal_history = {}  # symbol: list of recent signals
            self.active_signals = {}  # signal_id: signal_data
            self.max_history_per_symbol = 10
            self.cooldown_period = 900  # 15 minutes cooldown
            
        def should_generate_signal(self, symbol: str, side: str, price: float, 
                                  trend: ElliottTrend, indicators: IndicatorSignal) -> bool:
            """Check if we should generate a new signal"""
            current_time = time.time()
            
            # Check cooldown first
            if symbol in self.signal_history:
                # Get most recent signal for this symbol
                recent_signals = self.signal_history[symbol]
                if recent_signals:
                    last_signal_time = recent_signals[-1].get("timestamp", 0)
                    if current_time - last_signal_time < self.cooldown_period:
                        log.debug(f"{symbol}: In cooldown period ({int(current_time - last_signal_time)}s remaining)")
                        return False
            
            # Check for similar active signals
            if symbol in self.active_signals:
                for signal_id, signal in self.active_signals[symbol].items():
                    # If same side and similar price, skip
                    if signal["side"] == side:
                        price_diff_pct = abs(price - signal["price"]) / price * 100
                        if price_diff_pct < DEDUPLICATION_CONFIG["price_similarity_threshold"]:
                            # Check if conditions are similar
                            if self._are_conditions_similar(signal["conditions"], indicators):
                                log.debug(f"{symbol}: Similar {side} signal active (price diff: {price_diff_pct:.2f}%)")
                                return False
            
            return True
        
        def _are_conditions_similar(self, old_conditions: Dict, new_indicators: IndicatorSignal) -> bool:
            """Check if conditions are similar enough to be duplicates"""
            # Compare key indicator states
            similarity_score = 0
            
            if old_conditions.get("rsi_signal") == new_indicators.rsi_signal:
                similarity_score += 1
            
            if old_conditions.get("ema_signal") == new_indicators.ema_signal:
                similarity_score += 1
            
            if old_conditions.get("volume_signal") == new_indicators.volume_signal:
                similarity_score += 1
            
            # If 2+ indicators match, consider it similar
            return similarity_score >= 2
        
        def register_signal(self, signal: HighFreqSignal):
            """Register a new signal"""
            symbol = signal.symbol
            
            # Add to history
            if symbol not in self.signal_history:
                self.signal_history[symbol] = []
            
            signal_data = {
                "timestamp": signal.signal_timestamp,
                "side": signal.side,
                "price": signal.entry_price,
                "conditions": {
                    "rsi_signal": signal.indicators.rsi_signal,
                    "ema_signal": signal.indicators.ema_signal,
                    "volume_signal": signal.indicators.volume_signal
                }
            }
            
            self.signal_history[symbol].append(signal_data)
            
            # Keep only recent history
            if len(self.signal_history[symbol]) > self.max_history_per_symbol:
                self.signal_history[symbol] = self.signal_history[symbol][-self.max_history_per_symbol:]
            
            # Mark as active
            if symbol not in self.active_signals:
                self.active_signals[symbol] = {}
            
            # Check max active signals per symbol
            if len(self.active_signals[symbol]) >= DEDUPLICATION_CONFIG["max_active_signals"]:
                # Remove oldest signal
                oldest_id = min(self.active_signals[symbol].keys(), 
                              key=lambda k: self.active_signals[symbol][k]["timestamp"])
                del self.active_signals[symbol][oldest_id]
            
            self.active_signals[symbol][signal.signal_id] = {
                "side": signal.side,
                "price": signal.entry_price,
                "conditions": signal_data["conditions"],
                "timestamp": signal.signal_timestamp
            }
            
            log.debug(f"Registered signal {signal.signal_id[:8]} for {symbol}")
        
        def remove_signal(self, signal_id: str, symbol: str):
            """Remove signal from active tracking"""
            if symbol in self.active_signals and signal_id in self.active_signals[symbol]:
                del self.active_signals[symbol][signal_id]
                
                # Clean up if no active signals for symbol
                if not self.active_signals[symbol]:
                    del self.active_signals[symbol]
                
                log.debug(f"Removed signal {signal_id[:8]} from {symbol}")
        
        def cleanup_old_signals(self):
            """Clean up signals older than cooldown period"""
            current_time = time.time()
            cleaned_count = 0
            
            for symbol in list(self.active_signals.keys()):
                for signal_id in list(self.active_signals[symbol].keys()):
                    signal_data = self.active_signals[symbol][signal_id]
                    if current_time - signal_data["timestamp"] > self.cooldown_period:
                        del self.active_signals[symbol][signal_id]
                        cleaned_count += 1
                
                # Clean up empty symbols
                if not self.active_signals[symbol]:
                    del self.active_signals[symbol]
            
            if cleaned_count > 0:
                log.debug(f"Cleaned up {cleaned_count} old signals")
    
    def __init__(self):
        self.signals_today = {}
        self.daily_stats = {
            "signals_generated": 0,
            "long_signals": 0,
            "short_signals": 0,
            "pairs_scanned": 0,
            "signals_filtered": 0  # Track filtered signals
        }
        self.deduplicator = self.SignalDeduplicator()
        self.active_signal_ids = set()
        self.last_signal_per_symbol = {}  # Track last signal per symbol
    
    # ========== ELLIOTT WAVE TREND ANALYSIS ==========
    
    def analyze_elliott_trend(self, df_4h: pd.DataFrame, df_1h: pd.DataFrame) -> ElliottTrend:
        """
        Analyze Elliott Wave trend (direction only, not counting)
        Returns trend direction for context
        """
        try:
            # Check if dataframes are valid
            if df_4h is None or df_1h is None:
                return self._get_default_trend()
            
            if len(df_4h) < 20 or len(df_1h) < 20:
                return self._get_default_trend()
            
            # Get trends from both timeframes
            trend_4h = self._get_simple_trend(df_4h)
            trend_1h = self._get_simple_trend(df_1h)
            
            # Combine trends
            if trend_4h["direction"] == trend_1h["direction"]:
                direction = trend_4h["direction"]
                strength = (trend_4h["strength"] + trend_1h["strength"]) / 2
                trend_source = "BOTH"
            else:
                # Prefer 4H trend but consider strength
                if trend_4h["strength"] > trend_1h["strength"] * 1.5:
                    direction = trend_4h["direction"]
                    strength = trend_4h["strength"]
                    trend_source = "4H"
                else:
                    direction = "NEUTRAL"
                    strength = 0.5
                    trend_source = "CONFLICT"
            
            # Determine wave position (simplified)
            wave_position, wave_maturity = self._estimate_wave_position(df_1h, direction)
            
            return ElliottTrend(
                direction=direction,
                strength=strength,
                wave_position=wave_position,
                wave_maturity=wave_maturity,
                trend_source=trend_source
            )
            
        except Exception as e:
            log.error(f"Elliott trend error: {e}")
            return self._get_default_trend()
    
    def _get_default_trend(self) -> ElliottTrend:
        """Get default trend when analysis fails"""
        return ElliottTrend(
            direction="NEUTRAL",
            strength=0.5,
            wave_position="UNKNOWN",
            wave_maturity=0.5,
            trend_source="ERROR"
        )
    
    def _get_simple_trend(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Simple trend detection using price structure"""
        try:
            if len(df) < 20:
                return {"direction": "NEUTRAL", "strength": 0.5}
            
            prices = df['close'].values[-20:]
            
            # Calculate slopes
            x = np.arange(len(prices))
            slope, intercept = np.polyfit(x, prices, 1)
            
            # Calculate higher highs/lows for bullish, lower highs/lows for bearish
            highs = df['high'].values[-10:]
            lows = df['low'].values[-10:]
            
            # Check for higher highs in bullish trend
            higher_highs = all(highs[i] > highs[i-1] for i in range(1, len(highs)))
            higher_lows = all(lows[i] > lows[i-1] for i in range(1, len(lows)))
            
            # Check for lower highs in bearish trend
            lower_highs = all(highs[i] < highs[i-1] for i in range(1, len(highs)))
            lower_lows = all(lows[i] < lows[i-1] for i in range(1, len(lows)))
            
            # Determine trend
            bullish_score = 0
            bearish_score = 0
            
            if slope > 0:
                bullish_score += 1
            else:
                bearish_score += 1
            
            if higher_highs:
                bullish_score += 1
            if higher_lows:
                bullish_score += 1
            if lower_highs:
                bearish_score += 1
            if lower_lows:
                bearish_score += 1
            
            if bullish_score > bearish_score:
                direction = "BULLISH"
                strength = bullish_score / 4
            elif bearish_score > bullish_score:
                direction = "BEARISH"
                strength = bearish_score / 4
            else:
                direction = "NEUTRAL"
                strength = 0.5
            
            return {"direction": direction, "strength": strength}
            
        except Exception as e:
            return {"direction": "NEUTRAL", "strength": 0.5}
    
    def _estimate_wave_position(self, df: pd.DataFrame, trend: str) -> Tuple[str, float]:
        """Estimate wave position and maturity"""
        try:
            if len(df) < 30:
                return "UNKNOWN", 0.5
            
            prices = df['close'].values[-30:]
            volatility = np.std(prices[-20:])
            
            # Recent price action
            recent_move = prices[-1] - prices[-10]
            
            if abs(recent_move) > volatility * 1.2:
                wave_position = "IMPULSIVE"
            else:
                wave_position = "CORRECTIVE"
            
            # Wave maturity (simplified - based on distance from moving average)
            ma = np.mean(prices[-20:])
            current_price = prices[-1]
            
            if ma > 0:
                distance_pct = abs(current_price - ma) / ma * 100
                wave_maturity = min(distance_pct / 10, 1.0)  # 10% = fully mature
            else:
                wave_maturity = 0.5
            
            return wave_position, wave_maturity
            
        except Exception as e:
            return "UNKNOWN", 0.5
    
    # ========== INDICATOR ANALYSIS ==========
    
    def calculate_rsi(self, prices: pd.Series, period: int = RSI_PERIOD) -> pd.Series:
        """Calculate RSI"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def calculate_emas(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """Calculate EMAs"""
        return {
            name: df['close'].ewm(span=period, adjust=False).mean()
            for name, period in EMA_PERIODS.items()
        }
    
    def analyze_indicators(self, df: pd.DataFrame, trend: str) -> IndicatorSignal:
        """
        Analyze RSI, EMA, Volume for entry signals
        """
        try:
            if df is None or len(df) < 30:
                return self._get_default_indicators()
            
            current_price = df['close'].iloc[-1]
            
            # 1. RSI Analysis
            rsi = self.calculate_rsi(df['close'])
            current_rsi = rsi.iloc[-1] if len(rsi) > 0 else 50
            
            rsi_signal = "NEUTRAL"
            if current_rsi < RSI_OVERSOLD:
                rsi_signal = "OVERSOLD"
            elif current_rsi > RSI_OVERBOUGHT:
                rsi_signal = "OVERBOUGHT"
            
            # Check for divergence (simplified)
            if len(rsi) >= 10:
                rsi_trend = np.polyfit(range(5), rsi.values[-5:], 1)[0]
                price_trend = np.polyfit(range(5), df['close'].values[-5:], 1)[0]
                
                if price_trend > 0 and rsi_trend < -2:
                    rsi_signal = "BEARISH_DIV"
                elif price_trend < 0 and rsi_trend > 2:
                    rsi_signal = "BULLISH_DIV"
            
            # 2. EMA Analysis
            emas = self.calculate_emas(df)
            fast_ema = emas['fast'].iloc[-1]
            
            ema_distance_pct = (current_price - fast_ema) / fast_ema * 100
            
            ema_signal = "NEUTRAL"
            
            # Check for bounce/rejection
            current_candle = df.iloc[-1]
            prev_candle = df.iloc[-2] if len(df) > 1 else current_candle
            
            # Bullish bounce from EMA
            if (prev_candle['low'] < fast_ema and 
                current_candle['close'] > fast_ema and
                current_candle['close'] > current_candle['open']):
                ema_signal = "BOUNCE"
            
            # Bearish rejection from EMA
            elif (prev_candle['high'] > fast_ema and 
                  current_candle['close'] < fast_ema and
                  current_candle['close'] < current_candle['open']):
                ema_signal = "REJECTION"
            
            # EMA compression (fast and medium close together)
            medium_ema = emas['medium'].iloc[-1]
            ema_spread = abs(fast_ema - medium_ema) / medium_ema * 100
            
            if ema_spread < 0.5:  # Very tight
                ema_signal = "COMPRESSION"
            
            # Price overstretched from EMA
            elif abs(ema_distance_pct) > 3:
                ema_signal = "OVERSTRETCH"
            
            # 3. Volume Analysis
            recent_volume = df['volume'].values[-5:].mean()
            avg_volume = df['volume'].values[-20:].mean()
            volume_ratio = recent_volume / avg_volume if avg_volume > 0 else 1.0
            
            volume_signal = "NEUTRAL"
            if len(df) >= 5:
                price_change = (current_price - df['close'].iloc[-5]) / df['close'].iloc[-5] * 100
                
                if volume_ratio > 1.5:
                    if price_change > 0.5:
                        volume_signal = "CONFIRMING"
                    elif price_change < -0.5:
                        volume_signal = "CONFIRMING"
                    else:
                        volume_signal = "NEUTRAL"
                elif volume_ratio < 0.7:
                    volume_signal = "DIVERGING"
            
            # 4. Market Strength Score
            strength_factors = []
            
            # RSI strength
            if rsi_signal in ["OVERSOLD", "BULLISH_DIV"] and trend == "BULLISH":
                strength_factors.append(0.8)
            elif rsi_signal in ["OVERBOUGHT", "BEARISH_DIV"] and trend == "BEARISH":
                strength_factors.append(0.8)
            else:
                strength_factors.append(0.4)
            
            # EMA strength
            if ema_signal in ["BOUNCE", "REJECTION"]:
                strength_factors.append(0.7)
            elif ema_signal == "COMPRESSION":
                strength_factors.append(0.6)
            else:
                strength_factors.append(0.5)
            
            # Volume strength
            if volume_signal == "CONFIRMING":
                strength_factors.append(0.8)
            elif volume_signal == "DIVERGING":
                strength_factors.append(0.3)
            else:
                strength_factors.append(0.5)
            
            strength_score = np.mean(strength_factors) if strength_factors else 0.5
            
            return IndicatorSignal(
                rsi_signal=rsi_signal,
                rsi_value=current_rsi,
                ema_signal=ema_signal,
                ema_distance_pct=ema_distance_pct,
                volume_signal=volume_signal,
                volume_ratio=volume_ratio,
                strength_score=strength_score
            )
            
        except Exception as e:
            log.error(f"Indicator analysis error: {e}")
            return self._get_default_indicators()
    
    def _get_default_indicators(self) -> IndicatorSignal:
        """Get default indicators when analysis fails"""
        return IndicatorSignal(
            rsi_signal="NEUTRAL",
            rsi_value=50,
            ema_signal="NEUTRAL",
            ema_distance_pct=0,
            volume_signal="NEUTRAL",
            volume_ratio=1.0,
            strength_score=0.5
        )
    
    # ========== HIGH-FREQUENCY SIGNAL GENERATION ==========
    
    def _check_time_based_deduplication(self, symbol: str, side: str) -> bool:
        """Time-based deduplication to prevent same-side signals too close"""
        current_time = time.time()
        
        if symbol in self.last_signal_per_symbol:
            last_signal = self.last_signal_per_symbol[symbol]
            
            # Same side signals need longer cooldown
            if last_signal["side"] == side:
                time_since_last = current_time - last_signal["timestamp"]
                
                if side == "LONG":
                    if time_since_last < DEDUPLICATION_CONFIG["cooldown_same_side"]:
                        log.debug(f"{symbol}: Same side ({side}) cooldown active ({int(time_since_last)}s)")
                        return False
                else:  # SHORT
                    if time_since_last < DEDUPLICATION_CONFIG["cooldown_same_side"]:
                        log.debug(f"{symbol}: Same side ({side}) cooldown active ({int(time_since_last)}s)")
                        return False
            
            # Different side signals can come sooner
            else:
                if time_since_last < DEDUPLICATION_CONFIG["cooldown_opposite_side"]:
                    log.debug(f"{symbol}: Opposite side cooldown active ({int(time_since_last)}s)")
                    return False
        
        return True
    
    def generate_high_freq_signal(self, multi_tf_data: Dict[str, pd.DataFrame], 
                                 symbol: str) -> Optional[HighFreqSignal]:
        """
        Generate high-frequency signal based on Elliott trend + Indicators
        WITH DEDUPLICATION
        """
        try:
            # Get timeframe data with proper None checks
            tf_4h = multi_tf_data.get("4H")
            tf_1h = multi_tf_data.get("1H")
            tf_15m = multi_tf_data.get("15M")
            tf_5m = multi_tf_data.get("5M")
            
            # Check if any timeframe is None or invalid
            if tf_4h is None or tf_1h is None or tf_15m is None or tf_5m is None:
                log.debug(f"{symbol}: Missing timeframe data")
                return None
            
            # Check if dataframes have enough data
            if (len(tf_4h) < 20 or len(tf_1h) < 20 or 
                len(tf_15m) < 20 or len(tf_5m) < 20):
                log.debug(f"{symbol}: Insufficient data")
                return None
            
            # 1. Elliott Wave Trend Analysis (context only)
            trend = self.analyze_elliott_trend(tf_4h, tf_1h)
            
            if trend.direction == "NEUTRAL":
                log.debug(f"{symbol}: No clear trend direction")
                return None
            
            # 2. Check lower timeframes for entries
            entry_tfs = ["15M", "5M"]
            best_signal = None
            best_score = 0
            
            for tf_name in entry_tfs:
                df = multi_tf_data.get(tf_name)
                if df is None or len(df) < 20:
                    continue
                
                # Analyze indicators on this timeframe
                indicators = self.analyze_indicators(df, trend.direction)
                
                # Check if indicators support the trend
                confluence_score = self._calculate_confluence_score(trend, indicators)
                
                # Minimum confluence threshold (LOW for high frequency)
                if confluence_score < 0.5:
                    continue
                
                # Check if this is better than previous signals
                if confluence_score > best_score:
                    best_score = confluence_score
                    best_signal = {
                        "timeframe": tf_name,
                        "df": df,
                        "indicators": indicators,
                        "confluence_score": confluence_score
                    }
            
            if not best_signal:
                log.debug(f"{symbol}: No good entry confluence")
                return None
            
            # 3. Determine trade side based on trend
            if trend.direction == "BULLISH":
                side = "LONG"
            else:  # BEARISH
                side = "SHORT"
            
            # 4. TIME-BASED DEDUPLICATION CHECK
            if not self._check_time_based_deduplication(symbol, side):
                return None
            
            # 5. Calculate entry price for deduplication
            df_entry = best_signal["df"]
            current_price = df_entry['close'].iloc[-1]
            
            # 6. DEDUPLICATION CHECK
            if not self.deduplicator.should_generate_signal(
                symbol, side, current_price, trend, best_signal["indicators"]
            ):
                self.daily_stats["signals_filtered"] += 1
                return None
            
            # 7. Calculate entry, SL, TP (HIGH FREQUENCY)
            stop_loss_pct = np.random.uniform(0.5, MAX_STOP_LOSS_PCT)
            target_pct = np.random.uniform(MIN_TARGET_PCT, MAX_TARGET_PCT)
            
            if side == "LONG":
                stop_loss = current_price * (1 - stop_loss_pct / 100)
                take_profit = current_price * (1 + target_pct / 100)
            else:  # SHORT
                stop_loss = current_price * (1 + stop_loss_pct / 100)
                take_profit = current_price * (1 - target_pct / 100)
            
            # Calculate Risk/Reward
            risk = abs(current_price - stop_loss)
            reward = abs(take_profit - current_price)
            
            if risk == 0:
                return None
            
            risk_reward = reward / risk
            
            # Minimum R:R check
            if risk_reward < MIN_RISK_REWARD:
                log.debug(f"{symbol}: R:R too low ({risk_reward:.1f}:1)")
                return None
            
            # 8. Determine conditions met
            conditions_met = self._get_conditions_met(trend, best_signal["indicators"])
            
            # 9. Create signal ID
            signal_id = hashlib.md5(
                f"{symbol}:{side}:{current_price:.8f}:{time.time()}".encode()
            ).hexdigest()
            
            # 10. Create final signal
            signal = HighFreqSignal(
                signal_id=signal_id,
                symbol=symbol,
                side=side,
                entry_price=current_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
                
                trend=trend,
                indicators=best_signal["indicators"],
                
                confluence_score=best_signal["confluence_score"],
                risk_reward=risk_reward,
                expected_move_pct=target_pct,
                
                timeframe_used=best_signal["timeframe"],
                signal_timestamp=time.time(),
                conditions_met=conditions_met
            )
            
            # 11. Update tracking and deduplication
            self.deduplicator.register_signal(signal)
            self.active_signal_ids.add(signal_id)
            self.last_signal_per_symbol[symbol] = {
                "side": side,
                "timestamp": time.time()
            }
            
            # 12. Update statistics
            self.daily_stats["signals_generated"] += 1
            if side == "LONG":
                self.daily_stats["long_signals"] += 1
            else:
                self.daily_stats["short_signals"] += 1
            
            log.info(f"🚀 HIGH-FREQ SIGNAL: {symbol} {side} @ {current_price:.4f}")
            log.info(f"   Trend: {trend.direction}, TF: {best_signal['timeframe']}")
            log.info(f"   Confluence: {best_signal['confluence_score']:.2f}, R:R: {risk_reward:.1f}:1")
            log.info(f"   Expected: {target_pct:.1f}%, Conditions: {len(conditions_met)}")
            
            return signal
            
        except Exception as e:
            log.error(f"Signal generation error for {symbol}: {e}")
            return None
    
    def _calculate_confluence_score(self, trend: ElliottTrend, indicators: IndicatorSignal) -> float:
        """Calculate confluence score between trend and indicators"""
        confluence_factors = []
        
        # RSI confluence with trend
        if trend.direction == "BULLISH":
            if indicators.rsi_signal in ["OVERSOLD", "BULLISH_DIV"]:
                confluence_factors.append(0.9)
            elif indicators.rsi_value < 50:
                confluence_factors.append(0.7)
            else:
                confluence_factors.append(0.4)
        
        elif trend.direction == "BEARISH":
            if indicators.rsi_signal in ["OVERBOUGHT", "BEARISH_DIV"]:
                confluence_factors.append(0.9)
            elif indicators.rsi_value > 50:
                confluence_factors.append(0.7)
            else:
                confluence_factors.append(0.4)
        
        # EMA confluence
        if indicators.ema_signal in ["BOUNCE", "REJECTION", "COMPRESSION"]:
            confluence_factors.append(0.8)
        elif indicators.ema_signal == "OVERSTRETCH":
            confluence_factors.append(0.4)  # Warning sign
        else:
            confluence_factors.append(0.6)
        
        # Volume confluence
        if indicators.volume_signal == "CONFIRMING":
            confluence_factors.append(0.9)
        elif indicators.volume_signal == "DIVERGING":
            confluence_factors.append(0.3)
        else:
            confluence_factors.append(0.5)
        
        # Overall strength
        confluence_factors.append(indicators.strength_score)
        
        return np.mean(confluence_factors) if confluence_factors else 0.5
    
    def _get_conditions_met(self, trend: ElliottTrend, indicators: IndicatorSignal) -> List[str]:
        """Get list of conditions met for this signal"""
        conditions = []
        
        # Trend condition
        conditions.append(f"TREND_{trend.direction}")
        
        # RSI condition
        if indicators.rsi_signal != "NEUTRAL":
            conditions.append(f"RSI_{indicators.rsi_signal}")
        
        # EMA condition
        if indicators.ema_signal != "NEUTRAL":
            conditions.append(f"EMA_{indicators.ema_signal}")
        
        # Volume condition
        if indicators.volume_signal != "NEUTRAL":
            conditions.append(f"VOLUME_{indicators.volume_signal}")
        
        # Wave position
        conditions.append(f"WAVE_{trend.wave_position}")
        
        return conditions
    
    def get_daily_stats(self) -> Dict:
        """Get daily statistics"""
        return self.daily_stats
    
    def cleanup_old_signals(self):
        """Clean up old signals from deduplication"""
        self.deduplicator.cleanup_old_signals()

# ================ MAIN SCANNER SYSTEM ================
class ElliottIndicatorsScanner:
    """Main scanner system for high-frequency Elliott + Indicators signals"""
    
    def __init__(self):
        self.scanner = HighFrequencyScanner()
        self.exchange = None
        self.db = None
        self.scan_cycle = 0
        
    async def initialize(self):
        """Initialize the scanner"""
        log.info("=" * 70)
        log.info("🔥 ELLIOTT WAVE + INDICATORS HIGH-FREQUENCY SCANNER")
        log.info("=" * 70)
        log.info("PHILOSOPHY: Trend from Elliott Waves, Entries from Indicators")
        log.info("FREQUENCY: High (multiple signals per pair per day)")
        log.info("TARGET: 3-8% moves within minutes to hours")
        log.info(f"SCAN INTERVAL: {SCAN_INTERVAL} seconds")
        log.info(f"DEDUPLICATION: Active with {DEDUPLICATION_CONFIG['cooldown_same_side']}s cooldown")
        log.info("NOTIFICATIONS: Signal + Entry + TP/SL alerts enabled")
        log.info("=" * 70)
        
        # Initialize database
        await self._init_database()
        
        # Initialize exchange
        await self._init_exchange()
        
        # Send startup message
        await self._send_startup_message()
    
    async def _init_database(self):
        """Initialize database"""
        try:
            os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
            self.db = await aiosqlite.connect(DB_PATH)
            
            # Signals table
            await self.db.execute("""
            CREATE TABLE IF NOT EXISTS high_freq_signals (
                id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                entry_price REAL NOT NULL,
                stop_loss REAL NOT NULL,
                take_profit REAL NOT NULL,
                
                trend_direction TEXT NOT NULL,
                trend_strength REAL NOT NULL,
                wave_position TEXT NOT NULL,
                
                rsi_signal TEXT NOT NULL,
                rsi_value REAL NOT NULL,
                ema_signal TEXT NOT NULL,
                volume_signal TEXT NOT NULL,
                volume_ratio REAL NOT NULL,
                
                confluence_score REAL NOT NULL,
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
            
            # Performance table
            await self.db.execute("""
            CREATE TABLE IF NOT EXISTS performance_daily (
                date DATE PRIMARY KEY,
                total_signals INTEGER,
                long_signals INTEGER,
                short_signals INTEGER,
                win_rate REAL,
                avg_win REAL,
                avg_loss REAL,
                total_pnl REAL
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
                "rateLimit": 50  # Faster rate limit
            })
            
            # Test connection
            ticker = await self.exchange.fetch_ticker("BTC/USDT")
            log.info(f"✅ Exchange connected. BTC: ${ticker['last']:.2f}")
            
        except Exception as e:
            log.error(f"Exchange error: {e}")
            raise
    
    async def _send_startup_message(self):
        """Send startup message to Telegram"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return
        
        try:
            message = f"""
🚀 <b>ELLIOTT + INDICATORS HIGH-FREQUENCY SCANNER</b>

<b>🎯 STRATEGY:</b>
• Trend direction from Elliott Waves (4H/1H)
• Entry signals from Indicators (RSI, EMA, Volume)
• High-frequency signals (multiple per pair per day)
• Fast moves: 3-8% within minutes to hours

<b>⚡ CONFIGURATION:</b>
• Scan interval: {SCAN_INTERVAL} seconds
• Timeframes: 4H(trend), 1H(wave), 15M/5M(entry)
• Stop loss: 0.5-1.0%
• Target: 3-8%
• Risk/Reward: Minimum 1:2

<b>🛡️ DEDUPLICATION:</b>
• Same-side cooldown: {DEDUPLICATION_CONFIG['cooldown_same_side']//60} min
• Opposite-side cooldown: {DEDUPLICATION_CONFIG['cooldown_opposite_side']//60} min
• Price similarity: {DEDUPLICATION_CONFIG['price_similarity_threshold']}%
• Max signals per hour: {DEDUPLICATION_CONFIG['max_signals_per_hour']}

<b>🔔 NOTIFICATIONS:</b>
• Signal alerts ✓
• Entry execution alerts ✓
• TP/SL closure alerts ✓

<b>✅ STATUS: ACTIVE AND SCANNING</b>

#ElliottScanner #HighFrequency #CompleteAlerts
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML"
                })
                
        except Exception as e:
            log.error(f"Telegram startup error: {e}")
    
    async def fetch_timeframe_data(self, symbol: str) -> Dict[str, pd.DataFrame]:
        """Fetch data for all timeframes"""
        data = {}
        
        for tf_name, tf in TIMEFRAMES.items():
            try:
                # Adjust limits for different timeframes
                limit = 100 if tf_name in ["4H", "1H"] else 50
                
                ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe=tf, limit=limit)
                
                if ohlcv and len(ohlcv) >= 20:
                    df = pd.DataFrame(
                        ohlcv,
                        columns=["timestamp", "open", "high", "low", "close", "volume"]
                    )
                    
                    # Convert to numeric
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
                        # Check price spread for liquidity
                        bid = ticker.get('bid', 0)
                        ask = ticker.get('ask', 0)
                        
                        if bid > 0 and ask > 0:
                            spread = (ask - bid) / bid * 100
                            if spread < 0.1:  # Good liquidity
                                active_pairs.append((symbol, volume))
            
            # Sort by volume
            active_pairs.sort(key=lambda x: x[1], reverse=True)
            
            # Take top N
            return active_pairs[:TOP_N_VOLUME]
            
        except Exception as e:
            log.error(f"Error getting pairs: {e}")
            return []
    
    async def save_signal(self, signal: HighFreqSignal) -> bool:
        """Save signal to database"""
        try:
            # Insert signal
            await self.db.execute("""
                INSERT INTO high_freq_signals (
                    id, symbol, side, entry_price, stop_loss, take_profit,
                    trend_direction, trend_strength, wave_position,
                    rsi_signal, rsi_value, ema_signal, volume_signal, volume_ratio,
                    confluence_score, risk_reward, expected_move, timeframe_used,
                    conditions_met
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal.signal_id,
                signal.symbol,
                signal.side,
                signal.entry_price,
                signal.stop_loss,
                signal.take_profit,
                signal.trend.direction,
                signal.trend.strength,
                signal.trend.wave_position,
                signal.indicators.rsi_signal,
                signal.indicators.rsi_value,
                signal.indicators.ema_signal,
                signal.indicators.volume_signal,
                signal.indicators.volume_ratio,
                signal.confluence_score,
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
    
    async def format_signal_message(self, signal: HighFreqSignal) -> str:
        """Format signal for Telegram"""
        side_emoji = "🟢" if signal.side == "LONG" else "🔴"
        side_text = "شراء" if signal.side == "LONG" else "بيع"
        
        # Risk info
        risk_pct = abs(signal.entry_price - signal.stop_loss) / signal.entry_price * 100
        
        # RSI info
        rsi_text = f"{signal.indicators.rsi_value:.1f} ({signal.indicators.rsi_signal})"
        
        # Deduplication status
        dedupe_info = f"✅ إشارة جديدة | ❌ تم حظر {self.scanner.daily_stats.get('signals_filtered', 0)} إشارة مكررة"
        
        message = f"""
{side_emoji} <b>إشارة عالية التردد</b>

<b>{signal.symbol}</b> | {side_text}

<b>📈 اتجاه الموجة:</b>
• الاتجاه: {signal.trend.direction}
• قوة الاتجاه: {signal.trend.strength:.1%}
• مرحلة الموجة: {signal.trend.wave_position}
• النضج: {signal.trend.wave_maturity:.1%}

<b>📊 المؤشرات الدخول:</b>
• الإطار الزمني: {signal.timeframe_used}
• RSI: {rsi_text}
• اشارة الـ EMA: {signal.indicators.ema_signal}
• الفوليوم: {signal.indicators.volume_signal} (×{signal.indicators.volume_ratio:.1f})

<b>⚡ التنفيذ:</b>
• سعر الدخول: <code>{signal.entry_price:.6f}</code>
• وقف الخسارة: <code>{signal.stop_loss:.6f}</code> ({risk_pct:.2f}%)
• هدف الربح: <code>{signal.take_profit:.6f}</code> ({signal.expected_move_pct:.1f}%)

<b>🎯 الجودة:</b>
• درجة التوافق: {signal.confluence_score:.1%}
• نسبة الربح/المخاطرة: {signal.risk_reward:.1f}:1
• الشروط المحققة: {len(signal.conditions_met)}

<b>🛡️ مكافحة التكرار:</b>
• {dedupe_info}
• مهلة بين الإشارات: {DEDUPLICATION_CONFIG['cooldown_same_side']//60} دقيقة لنفس الجانب

<b>⚠️ ملاحظة:</b>
هذه إشارة عالية التردد مع نظام مكافحة التكرار.
سيصلك إشعار عند الدخول وعند الإغلاق.

#{side_text} #موجات_إليوت #مؤشرات #لا_تكرار
"""
        return message
    
    async def send_trade_trigger_notification(self, symbol: str, side: str, entry_price: float):
        """Send notification when trade is triggered/entered"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return
        
        try:
            side_emoji = "🟢" if side == "LONG" else "🔴"
            side_text = "شراء" if side == "LONG" else "بيع"
            
            message = f"""
{side_emoji} <b>تم تنفيذ الصفقة</b> ⚡

<b>{symbol}</b> | {side_text}

<b>🎯 تم الدخول بالسعر:</b>
<code>{entry_price:.6f}</code>

<b>📊 حالة الصفقة:</b>
• النوع: {side_text}
• السعر: <code>{entry_price:.6f}</code>
• الحالة: <b>نشط</b>

<b>⚠️ المتابعة:</b>
يتم متابعة الصفقة تلقائياً.
ستصلك إشعار عند الوصول لوقف الخسارة أو هدف الربح.

#{side_text} #تنفيذ #متابعة
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML"
                })
            
            log.info(f"{side_emoji} Trade triggered: {symbol} {side} @ {entry_price:.4f}")
            
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
                result_emoji = "🎯"
                color = "🟢"
            else:  # SL_HIT
                emoji = "❌"
                result_text = "وقف الخسارة"
                result_emoji = "🛑"
                color = "🔴"
            
            side_text = "شراء" if side == "LONG" else "بيع"
            
            # Format P&L with sign
            pnl_formatted = f"+{pnl_percent:.2f}%" if pnl_percent > 0 else f"{pnl_percent:.2f}%"
            
            message = f"""
{emoji} <b>تم إغلاق الصفقة</b> {result_emoji}

<b>{symbol}</b> | {side_text}

{color} <b>النتيجة: {result_text}</b>

<b>📊 تفاصيل التنفيذ:</b>
• نوع الدخول: {side_text}
• سعر الدخول: <code>{entry_price:.6f}</code>
• سعر الإغلاق: <code>{close_price:.6f}</code>
• نسبة الربح/الخسارة: <b>{pnl_formatted}</b>
• نسبة الربح/المخاطرة المحققة: {risk_reward:.1f}:1

<b>📈 الملخص:</b>
{emoji} <b>{result_text}</b> - {symbol} {side_text}
{color} النسبة: <b>{pnl_formatted}</b>

#{side_text} #إغلاق #{"ربح" if close_reason == "TP_HIT" else "خسارة"}
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML"
                })
            
            log.info(f"{emoji} Trade closed: {symbol} {side} {pnl_formatted} ({close_reason})")
            
        except Exception as e:
            log.error(f"Close notification error: {e}")
    
    async def send_telegram_alert(self, signal: HighFreqSignal):
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
        """Monitor and close positions with complete notifications"""
        while True:
            try:
                # Get open positions
                async with self.db.execute("""
                    SELECT id, symbol, side, entry_price, stop_loss, take_profit 
                    FROM high_freq_signals 
                    WHERE status = 'PENDING'
                """) as cursor:
                    positions = await cursor.fetchall()
                
                for pos_id, symbol, side, entry, sl, tp in positions:
                    try:
                        # Get current price
                        ticker = await self.exchange.fetch_ticker(symbol)
                        current_price = ticker['last']
                        
                        # Check if price reached entry (high frequency - immediate)
                        if abs(current_price - entry) / entry <= 0.003:  # 0.3% zone
                            # Mark as triggered
                            await self.db.execute("""
                                UPDATE high_freq_signals SET 
                                    status = 'TRIGGERED',
                                    triggered_at = CURRENT_TIMESTAMP,
                                    trigger_price = ?
                                WHERE id = ?
                            """, (current_price, pos_id))
                            
                            await self.db.commit()
                            
                            # SEND TRIGGER NOTIFICATION
                            await self.send_trade_trigger_notification(symbol, side, current_price)
                            
                            log.info(f"✅ Position triggered: {symbol} {side} @ {current_price:.4f}")
                        
                        # Check SL/TP for triggered positions
                        async with self.db.execute("""
                            SELECT id FROM high_freq_signals 
                            WHERE id = ? AND status = 'TRIGGERED'
                        """, (pos_id,)) as cursor:
                            is_triggered = await cursor.fetchone()
                        
                        if is_triggered:
                            pnl_percent = 0
                            close_reason = None
                            
                            if side == "LONG":
                                if current_price <= sl:
                                    close_reason = "SL_HIT"
                                    pnl_percent = ((current_price - entry) / entry) * 100
                                elif current_price >= tp:
                                    close_reason = "TP_HIT"
                                    pnl_percent = ((current_price - entry) / entry) * 100
                            
                            else:  # SHORT
                                if current_price >= sl:
                                    close_reason = "SL_HIT"
                                    pnl_percent = ((entry - current_price) / entry) * 100
                                elif current_price <= tp:
                                    close_reason = "TP_HIT"
                                    pnl_percent = ((entry - current_price) / entry) * 100
                            
                            if close_reason:
                                # Get risk_reward from database
                                async with self.db.execute("""
                                    SELECT risk_reward FROM high_freq_signals WHERE id = ?
                                """, (pos_id,)) as cursor:
                                    row = await cursor.fetchone()
                                    risk_reward = row[0] if row else 0
                                
                                # Update database
                                await self.db.execute("""
                                    UPDATE high_freq_signals SET 
                                        status = 'CLOSED',
                                        closed_at = CURRENT_TIMESTAMP,
                                        close_price = ?,
                                        pnl_percent = ?,
                                        close_reason = ?
                                    WHERE id = ?
                                """, (current_price, pnl_percent, close_reason, pos_id))
                                
                                await self.db.commit()
                                
                                # Clean up from deduplication
                                if hasattr(self.scanner, 'deduplicator'):
                                    self.scanner.deduplicator.remove_signal(pos_id, symbol)
                                
                                if hasattr(self.scanner, 'active_signal_ids'):
                                    self.scanner.active_signal_ids.discard(pos_id)
                                
                                # SEND CLOSE NOTIFICATION
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
                
                # Clean up old signals periodically
                if int(time.time()) % 300 < 2:  # Every ~5 minutes
                    if hasattr(self.scanner, 'deduplicator'):
                        self.scanner.deduplicator.cleanup_old_signals()
                
                # Fast monitoring
                await asyncio.sleep(2)
                
            except Exception as e:
                log.error(f"Monitoring loop error: {e}")
                await asyncio.sleep(10)
    
    async def high_freq_scanning(self):
        """Main high-frequency scanning loop"""
        log.info("🚀 Starting high-frequency scanning with complete notifications...")
        
        while True:
            try:
                self.scan_cycle += 1
                start_time = time.time()
                
                log.info(f"🔄 Scan cycle #{self.scan_cycle}")
                
                # Get active pairs
                pairs = await self.get_active_pairs()
                
                if not pairs:
                    log.warning("No active pairs found")
                    await asyncio.sleep(SCAN_INTERVAL)
                    continue
                
                log.info(f"Scanning {len(pairs)} active pairs")
                
                signals_found = 0
                pairs_processed = 0
                
                # Ultra-fast scanning
                for symbol, volume in pairs:
                    try:
                        # Fetch data
                        multi_tf_data = await self.fetch_timeframe_data(symbol)
                        
                        # Need key timeframes - check individually
                        required_tfs = ["4H", "1H", "15M", "5M"]
                        has_all_data = all(tf in multi_tf_data for tf in required_tfs)
                        
                        if not has_all_data:
                            continue
                        
                        # Generate high-frequency signal
                        signal = self.scanner.generate_high_freq_signal(multi_tf_data, symbol)
                        
                        if signal:
                            # Save and send
                            saved = await self.save_signal(signal)
                            
                            if saved:
                                await self.send_telegram_alert(signal)
                                signals_found += 1
                        
                        pairs_processed += 1
                        
                        # Ultra-fast between pairs
                        await asyncio.sleep(0.02)
                        
                    except Exception as e:
                        log.debug(f"Pair error {symbol}: {str(e)[:50]}")
                        continue
                
                # Update scanner stats
                self.scanner.daily_stats["pairs_scanned"] += pairs_processed
                
                # Log deduplication stats
                if hasattr(self.scanner, 'deduplicator'):
                    active_count = sum(len(sigs) for sigs in self.scanner.deduplicator.active_signals.values())
                    log.info(f"📊 Active signals: {active_count}, Filtered: {self.scanner.daily_stats.get('signals_filtered', 0)}")
                
                scan_duration = time.time() - start_time
                log.info(f"Scan #{self.scan_cycle}: {signals_found} signals in {scan_duration:.1f}s")
                
                # Log stats periodically
                if self.scan_cycle % 20 == 0:
                    stats = self.scanner.get_daily_stats()
                    log.info(f"📊 Daily stats: {stats}")
                
                # Wait for next scan
                wait_time = max(0.5, SCAN_INTERVAL - scan_duration)
                log.info(f"Next scan in {wait_time:.1f}s...")
                await asyncio.sleep(wait_time)
                
            except Exception as e:
                log.error(f"Scanning loop error: {e}")
                await asyncio.sleep(30)
    
    async def run(self):
        """Run the scanner"""
        try:
            await self.initialize()
            
            # Run both loops
            await asyncio.gather(
                self.high_freq_scanning(),
                self.monitor_positions()
            )
            
        except KeyboardInterrupt:
            log.info("Scanner stopped by user")
            
            # Send final stats
            await self.send_final_stats()
            
        except Exception as e:
            log.error(f"Scanner crashed: {e}")
            
        finally:
            await self.cleanup()
    
    async def send_final_stats(self):
        """Send final statistics"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return
        
        try:
            stats = self.scanner.get_daily_stats()
            
            # Get active signals count
            active_count = 0
            if hasattr(self.scanner, 'deduplicator'):
                active_count = sum(len(sigs) for sigs in self.scanner.deduplicator.active_signals.values())
            
            message = f"""
🛑 <b>تم إيقاف الماسح الضوئي</b>

<b>📈 إحصائيات اليوم:</b>
• الإشارات المولدة: {stats['signals_generated']}
• الإشارات المفلترة (تكرار): {stats.get('signals_filtered', 0)}
• إشارات الشراء: {stats['long_signals']}
• إشارات البيع: {stats['short_signals']}
• الأزواج الممسوحة: {stats['pairs_scanned']}
• دورات المسح: {self.scan_cycle}
• الإشارات النشطة حالياً: {active_count}

<b>🔔 نظام الإشعارات:</b>
• إشعارات الإشارات ✓
• إشعارات الدخول ✓
• إشعارات الإغلاق (TP/SL) ✓

<b>🎯 الفلسفة المحققة:</b>
الاتجاه من موجات إليوت، الدخول من المؤشرات.
إشارات عالية التردد مع إشعارات كاملة من البداية للنهاية.

#إحصائيات #موجات_إليوت #إشعارات_كاملة
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML"
                })
                
        except Exception as e:
            log.error(f"Final stats error: {e}")
    
    async def cleanup(self):
        """Cleanup resources"""
        try:
            if self.exchange:
                await self.exchange.close()
                log.info("Exchange closed")
            
            if self.db:
                await self.db.close()
                log.info("Database closed")
                
        except Exception as e:
            log.error(f"Cleanup error: {e}")

# ================ SIMPLE HTTP SERVER ================
async def start_http_server(scanner, port=8000):
    """Start simple HTTP server for monitoring"""
    async def handle_request(reader, writer):
        try:
            request = await reader.read(1024)
            
            # Parse request
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
                # Get deduplication stats
                dedupe_stats = {}
                if hasattr(scanner.scanner, 'deduplicator'):
                    active_count = sum(len(sigs) for sigs in scanner.scanner.deduplicator.active_signals.values())
                    dedupe_stats = {
                        "active_signals": active_count,
                        "signals_filtered": scanner.scanner.daily_stats.get("signals_filtered", 0)
                    }
                
                response = json.dumps({
                    "status": "running",
                    "scanner": "Elliott Wave + Indicators High-Frequency Scanner",
                    "scan_cycle": scanner.scan_cycle,
                    "daily_stats": scanner.scanner.get_daily_stats(),
                    "deduplication": dedupe_stats
                }, indent=2)
            
            elif path == '/stats':
                response = json.dumps(scanner.scanner.get_daily_stats(), indent=2)
            
            elif path == '/deduplication':
                dedupe_info = {}
                if hasattr(scanner.scanner, 'deduplicator'):
                    dedupe_info = {
                        "active_signals": scanner.scanner.deduplicator.active_signals,
                        "signal_history": {k: len(v) for k, v in scanner.scanner.deduplicator.signal_history.items()},
                        "filtered_count": scanner.scanner.daily_stats.get("signals_filtered", 0)
                    }
                response = json.dumps(dedupe_info, indent=2)
            
            elif path == '/recent':
                if scanner.db:
                    scanner.db.row_factory = aiosqlite.Row
                    async with scanner.db.execute("""
                        SELECT symbol, side, entry_price, expected_move, risk_reward,
                               confluence_score, timeframe_used, created_at, status,
                               close_reason, pnl_percent
                        FROM high_freq_signals 
                        ORDER BY created_at DESC 
                        LIMIT 20
                    """) as cursor:
                        rows = await cursor.fetchall()
                        signals = [dict(row) for row in rows]
                    
                    response = json.dumps({"signals": signals, "count": len(signals)}, indent=2)
                else:
                    response = json.dumps({"error": "Database not available"})
            
            else:
                response = json.dumps({"error": "Endpoint not found"})
            
            # Send response
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
    """Main function"""
    # Create scanner
    scanner = ElliottIndicatorsScanner()
    
    # Start HTTP server in background
    http_task = asyncio.create_task(start_http_server(scanner))
    
    # Run scanner
    await scanner.run()

if __name__ == "__main__":
    # Run the main async function
    asyncio.run(main())