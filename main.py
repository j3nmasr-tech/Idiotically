#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🏆 ULTIMATE HYBRID SCANNER v3.1 - ROMEOPT INSTITUTIONAL SEQUENCING 🏆
- STRICT 6-STEP ROMEOPT SEQUENCE
- YOUR EXACT OLD FILTERS & SCORING PRESERVED  
- NEW ROME-STYLE SIGNAL GENERATION
- UNIFIED SMART TP/SL SYSTEM
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
from pydantic import BaseModel
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

class SignalSide(Enum):
    BUY = "BUY"
    SELL = "SELL"

@dataclass
class ScannerConfig:
    # Core settings (from OLD system)
    SCAN_INTERVAL: int = 60
    TOP_N_SYMBOLS: int = 80
    MIN_VOLUME_USDT: float = 1000000
    MAX_SPREAD_PCT: float = 0.002
    
    # OLD FILTER SETTINGS
    MIN_SIGNAL_SCORE: int = 0
    COOLDOWN_MINUTES: int = 30
    MAX_SL_CLUSTER_HITS: int = 3
    
    # OLD WINNER FILTER SETTINGS (EXACT OLD BEHAVIOR)
    REQUIRE_BTC_ALIGNMENT: bool = False  # 🛠️ CHANGE THIS TO FALSE
    REQUIRE_HIGHER_TF_ALIGNMENT: bool = True
    REQUIRE_MOMENTUM_CONFIRMATION: bool = True
    REQUIRE_ZONE_QUALITY: bool = True
    AVOID_CHOPPY_MARKETS: bool = True
    USE_MARKET_REGIME: bool = False
    
    # OLD SCORING
    WINNER_BONUS: int = 5

# ==================== ROMEOPT TP/SL SYSTEM ====================

class RomeOptTPSL:
    """UNIFIED SMART PROFIT SYSTEM FOR ALL SIGNALS - ENHANCED WITH TIMEFRAME AWARENESS"""
    
    @staticmethod
    def calculate_rome_tp_sl(df: pd.DataFrame, symbol: str, side: str, entry: float, context: Dict) -> Tuple[float, float, float, float]:
        """ENHANCED: TIMEFRAME-AWARE SMART TP/SL - PRESERVES ALL EXISTING LOGIC"""
        try:
            current_price = entry
            tf = context.get('tf', '15m')
            
            # Get ATR with enhanced fallback
            atr_val = RomeOptTPSL.rome_atr(df, 14) 
            if atr_val == 0 or df is None or len(df) < 14:
                # Smart fallback based on timeframe
                if tf in ['1m', '3m']:
                    atr_val = current_price * 0.004  # 0.4% for scalping
                elif tf in ['5m', '15m']:
                    atr_val = current_price * 0.008  # 0.8% for short-term
                else:  # 30m, 1h+
                    atr_val = current_price * 0.012  # 1.2% for longer-term
            
            # 🎯 ENHANCED TIMEFRAME-SPECIFIC PROTECTION
            if tf in ['1m', '3m']:
                min_atr = current_price * 0.003  # 0.3% minimum for scalping
                atr_multiplier = 1.2  # Tighter but safe
            elif tf in ['5m', '15m']:
                min_atr = current_price * 0.006  # 0.6% minimum
                atr_multiplier = 1.5
            else:  # 30m, 1h+
                min_atr = current_price * 0.009  # 0.9% minimum  
                atr_multiplier = 2.0
                
            atr_val = max(atr_val, min_atr)
            
            if side == "BUY":
                return RomeOptTPSL._calculate_bullish_targets_enhanced(df, current_price, atr_val, atr_multiplier, tf)
            else:
                return RomeOptTPSL._calculate_bearish_targets_enhanced(df, current_price, atr_val, atr_multiplier, tf)
                
        except Exception as e:
            logging.error(f"Rome TP/SL error: {e}")
            # 🛡️ ENHANCED TIMEFRAME-AWARE FALLBACK
            tf = context.get('tf', '15m')
            if tf in ['1m', '3m']:
                if side == "BUY":
                    return entry * 0.994, entry * 1.006, entry * 1.012, entry * 1.020  # 0.6% stop → 0.6%/1.2%/2.0%
                else:
                    return entry * 1.006, entry * 0.994, entry * 0.988, entry * 0.980  # 0.6% stop → 0.6%/1.2%/2.0%
            elif tf in ['5m', '15m']:
                if side == "BUY":
                    return entry * 0.990, entry * 1.010, entry * 1.020, entry * 1.030  # 1.0% stop → 1.0%/2.0%/3.0%
                else:
                    return entry * 1.010, entry * 0.990, entry * 0.980, entry * 0.970  # 1.0% stop → 1.0%/2.0%/3.0%
            else:  # 30m+
                if side == "BUY":
                    return entry * 0.985, entry * 1.015, entry * 1.025, entry * 1.035  # 1.5% stop → 1.5%/2.5%/3.5%
                else:
                    return entry * 1.015, entry * 0.985, entry * 0.975, entry * 0.965  # 1.5% stop → 1.5%/2.5%/3.5%

    @staticmethod
    def _calculate_bullish_targets_enhanced(df: pd.DataFrame, entry: float, atr: float, atr_multiplier: float, tf: str) -> Tuple[float, float, float, float]:
        """BULLISH: ENHANCED with timeframe-aware parameters"""
        # 🛡️ IMPROVED SWING LOW DETECTION (your existing logic preserved)
        swing_lows = RomeOptTPSL._find_significant_swing_lows(df.tail(50))
        if not swing_lows:
            # Enhanced fallback with timeframe consideration
            recent_low = df["low"].tail(15).min()
            sl = recent_low - (atr * atr_multiplier * 0.8)  # Use multiplier
        else:
            # Use the most recent significant swing low
            recent_swing_low = min(swing_lows[-2:])
            sl = recent_swing_low - (atr * atr_multiplier * 0.5)  # Use multiplier
            
        # 🛡️ ENHANCED MINIMUM STOP DISTANCE (timeframe-aware)
        if tf in ['1m', '3m']:
            min_stop_distance = entry * 0.005  # 0.5% minimum for scalping
        elif tf in ['5m', '15m']:
            min_stop_distance = entry * 0.008  # 0.8% minimum
        else:
            min_stop_distance = entry * 0.012  # 1.2% minimum
            
        current_stop_distance = entry - sl
        if current_stop_distance < min_stop_distance:
            sl = entry - min_stop_distance

        # TAKE PROFIT TARGETS (your existing logic preserved)
        swing_highs = RomeOptTPSL._find_significant_swing_highs(df.tail(30))
        recent_swing_high = max(swing_highs[-3:]) if swing_highs else entry * 1.015
        tp1 = recent_swing_high
        
        equal_highs = RomeOptTPSL._find_equal_highs(df.tail(30))
        if equal_highs:
            potential_tp2 = max(equal_highs[-2:])
            tp2 = max(potential_tp2, tp1 * 1.008)
        else:
            tp2 = max(entry + (atr * 2.2), tp1 * 1.008)
            
        tp3 = max(tp2 + (atr * 1.8), tp2 * 1.006)
        
        return sl, tp1, tp2, tp3

    @staticmethod
    def _calculate_bearish_targets_enhanced(df: pd.DataFrame, entry: float, atr: float, atr_multiplier: float, tf: str) -> Tuple[float, float, float, float]:
        """BEARISH: ENHANCED with timeframe-aware parameters"""
        swing_highs = RomeOptTPSL._find_significant_swing_highs(df.tail(50))
        if not swing_highs:
            recent_high = df["high"].tail(15).max()
            sl = recent_high + (atr * atr_multiplier * 0.8)  # Use multiplier
        else:
            recent_swing_high = max(swing_highs[-2:])
            sl = recent_swing_high + (atr * atr_multiplier * 0.5)  # Use multiplier
            
        # 🛡️ ENHANCED MINIMUM STOP DISTANCE (timeframe-aware)
        if tf in ['1m', '3m']:
            min_stop_distance = entry * 0.005  # 0.5% minimum for scalping
        elif tf in ['5m', '15m']:
            min_stop_distance = entry * 0.008  # 0.8% minimum
        else:
            min_stop_distance = entry * 0.012  # 1.2% minimum
            
        current_stop_distance = sl - entry
        if current_stop_distance < min_stop_distance:
            sl = entry + min_stop_distance

        swing_lows = RomeOptTPSL._find_significant_swing_lows(df.tail(30))
        recent_swing_low = min(swing_lows[-3:]) if swing_lows else entry * 0.985
        tp1 = recent_swing_low
        
        equal_lows = RomeOptTPSL._find_equal_lows(df.tail(30))
        if equal_lows:
            potential_tp2 = min(equal_lows[-2:])
            tp2 = min(potential_tp2, tp1 * 0.992)
        else:
            tp2 = min(entry - (atr * 2.2), tp1 * 0.992)
            
        tp3 = min(tp2 - (atr * 1.8), tp2 * 0.994)
        
        return sl, tp1, tp2, tp3

    # ✅ ALL YOUR EXISTING METHODS PRESERVED EXACTLY AS IS:
    @staticmethod
    def _find_significant_swing_lows(df: pd.DataFrame, lookback: int = 5, min_change: float = 0.003) -> List[float]:
        """Find ONLY significant swing lows (filters noise) - PRESERVED"""
        if len(df) < lookback * 2 + 1:
            return []
            
        significant_lows = []
        for i in range(lookback, len(df) - lookback):
            current_low = df["low"].iloc[i]
            
            # Check if it's a local minimum
            is_local_min = (current_low == df["low"].iloc[i-lookback:i+lookback+1].min())
            
            if is_local_min:
                # Check if this swing is significant (not just noise)
                left_avg = df["low"].iloc[i-lookback:i].mean()
                right_avg = df["low"].iloc[i+1:i+lookback+1].mean()
                avg_around = (left_avg + right_avg) / 2
                
                price_change = abs(avg_around - current_low) / current_low
                if price_change >= min_change:  # At least 0.3% change
                    significant_lows.append(current_low)
                    
        return significant_lows if significant_lows else [df["low"].min()]

    @staticmethod
    def _find_significant_swing_highs(df: pd.DataFrame, lookback: int = 5, min_change: float = 0.003) -> List[float]:
        """Find ONLY significant swing highs (filters noise) - PRESERVED"""
        if len(df) < lookback * 2 + 1:
            return []
            
        significant_highs = []
        for i in range(lookback, len(df) - lookback):
            current_high = df["high"].iloc[i]
            
            is_local_max = (current_high == df["high"].iloc[i-lookback:i+lookback+1].max())
            
            if is_local_max:
                left_avg = df["high"].iloc[i-lookback:i].mean()
                right_avg = df["high"].iloc[i+1:i+lookback+1].mean()
                avg_around = (left_avg + right_avg) / 2
                
                price_change = abs(avg_around - current_high) / current_high
                if price_change >= min_change:
                    significant_highs.append(current_high)
                    
        return significant_highs if significant_highs else [df["high"].max()]

    # 🔄 KEEP ALL YOUR EXISTING METHODS EXACTLY THE SAME:
    @staticmethod
    def _find_swing_lows(df: pd.DataFrame, lookback: int = 3) -> List[float]:
        """Use significant swing detection - PRESERVED"""
        return RomeOptTPSL._find_significant_swing_lows(df, lookback)

    @staticmethod
    def _find_swing_highs(df: pd.DataFrame, lookback: int = 3) -> List[float]:
        """Use significant swing detection - PRESERVED"""
        return RomeOptTPSL._find_significant_swing_highs(df, lookback)

    @staticmethod
    def _find_equal_highs(df: pd.DataFrame) -> List[float]:
        """Find recent equal highs (liquidity levels) - PRESERVED"""
        if len(df) < 10:
            return []
            
        recent_highs = df["high"].tail(10).values
        equal_highs = []
        
        for i in range(len(recent_highs)):
            for j in range(i+1, len(recent_highs)):
                if abs(recent_highs[i] - recent_highs[j]) < np.mean(recent_highs) * 0.002:
                    equal_highs.append(max(recent_highs[i], recent_highs[j]))
                    
        return list(set(equal_highs))

    @staticmethod
    def _find_equal_lows(df: pd.DataFrame) -> List[float]:
        """Find recent equal lows (liquidity levels) - PRESERVED"""
        if len(df) < 10:
            return []
            
        recent_lows = df["low"].tail(10).values
        equal_lows = []
        
        for i in range(len(recent_lows)):
            for j in range(i+1, len(recent_lows)):
                if abs(recent_lows[i] - recent_lows[j]) < np.mean(recent_lows) * 0.002:
                    equal_lows.append(min(recent_lows[i], recent_lows[j]))
                    
        return list(set(equal_lows))

    @staticmethod
    def rome_atr(df: pd.DataFrame, period: int = 14) -> float:
        """RomeOPT ATR calculation - PRESERVED"""
        if df is None or len(df) < period:
            return 0.0
            
        high, low, close = df["high"], df["low"], df["close"]
        tr1 = high - low
        tr2 = (high - close.shift(1)).abs()
        tr3 = (low - close.shift(1)).abs()
        true_range = pd.DataFrame({'tr1': tr1, 'tr2': tr2, 'tr3': tr3}).max(axis=1)
        return true_range.rolling(period).mean().iloc[-1]

# ==================== ROMEOPT INSTITUTIONAL SEQUENCING ====================

class RomeSMCAnalyzer:
    """STRICT ROMEOPT INSTITUTIONAL SEQUENCING LOGIC"""
    
    def __init__(self):
        self.sequence_complete = False
        self.current_step = 0
        self.rejection_reason = ""
        
    def generate_signal(self, df: pd.DataFrame, symbol: str, context=None) -> Optional[Dict]:
        """MAIN ENTRY POINT FOLLOWING ROMEOPT SEQUENCING"""
        if context is None:
            context = {}
            
        self.sequence_complete = False
        self.current_step = 0
        
        if df is None or len(df) < 20:
            return None

        try:
            # 🔥 STEP 1: Liquidity Sweep Condition
            sweep_result = self._check_liquidity_sweep(df)
            if not sweep_result["valid"]:
                self.rejection_reason = f"Liquidity sweep failed: {sweep_result['reason']}"
                return None
            self.current_step = 1
            
            # 🔥 STEP 2: Displacement Condition  
            displacement_result = self._check_displacement(df, sweep_result)
            if not displacement_result["valid"]:
                self.rejection_reason = f"Displacement failed: {displacement_result['reason']}"
                return None
            self.current_step = 2
            
            # 🔥 STEP 3: Retracement Into Zone
            zone_result = self._check_retracement_zone(df, displacement_result, context)
            if not zone_result["valid"]:
                self.rejection_reason = f"Retracement zone failed: {zone_result['reason']}"
                return None
            self.current_step = 3
            
            # 🔥 STEP 4: Premium/Discount Filter
            equilibrium_result = self._check_premium_discount(df, zone_result, context)
            if not equilibrium_result["valid"]:
                self.rejection_reason = f"Premium/discount filter failed: {equilibrium_result['reason']}"
                return None
            self.current_step = 4
            
            # 🔥 STEP 5: HTF Bias Alignment
            htf_result = self._check_htf_alignment(df, equilibrium_result, context)
            if not htf_result["valid"]:
                self.rejection_reason = f"HTF alignment failed: {htf_result['reason']}"
                return None
            self.current_step = 5
            
            # 🔥 STEP 6: Momentum & Volatility Confirmation
            momentum_result = self._check_momentum_volatility(df, htf_result, context)
            if not momentum_result["valid"]:
                self.rejection_reason = f"Momentum/volatility failed: {momentum_result['reason']}"
                return None
            self.current_step = 6
            
            # ✅ ALL ROME CONDITIONS MET - GENERATE SIGNAL
            self.sequence_complete = True
            return self._format_rome_signal(momentum_result, symbol, context, df)
            
        except Exception as e:
            logging.error(f"Rome sequencing error for {symbol}: {e}")
            return None

    def _check_liquidity_sweep(self, df: pd.DataFrame) -> Dict:
        """STEP 1: Check for valid liquidity sweep/stop-run"""
        if len(df) < 10:
            return {"valid": False, "reason": "Insufficient data"}
            
        recent_candles = df.iloc[-10:]
        
        for i in range(1, len(recent_candles)):
            current = recent_candles.iloc[i]
            previous = recent_candles.iloc[i-1]
            lookback_candles = recent_candles.iloc[:i]
            
            # Sweep of equal highs
            if self._is_equal_high_sweep(current, previous, lookback_candles):
                return {
                    "valid": True, 
                    "type": "equal_high_sweep", 
                    "direction": "bearish",
                    "sweep_index": -len(recent_candles) + i
                }
            
            # Sweep of equal lows  
            if self._is_equal_low_sweep(current, previous, lookback_candles):
                return {
                    "valid": True, 
                    "type": "equal_low_sweep", 
                    "direction": "bullish",
                    "sweep_index": -len(recent_candles) + i
                }
            
            # Stop-run wick above/below previous swing
            stop_run = self._is_stop_run_sweep(current, df)
            if stop_run["valid"]:
                stop_run["sweep_index"] = -len(recent_candles) + i
                return stop_run
        
        return {"valid": False, "reason": "No liquidity sweep detected"}

    def _is_equal_high_sweep(self, current, previous, lookback_candles) -> bool:
        """Detect sweep of equal highs"""
        if current["high"] <= previous["high"]:
            return False
            
        recent_highs = lookback_candles["high"].tail(5)
        if len(recent_highs) == 0:
            return False
            
        atr_val = self._calculate_atr(lookback_candles)
        threshold = atr_val * 0.1 if atr_val else current["high"] * 0.001
        
        for high_val in recent_highs:
            if abs(current["high"] - high_val) < threshold:
                return True
        return False

    def _is_equal_low_sweep(self, current, previous, lookback_candles) -> bool:
        """Detect sweep of equal lows"""
        if current["low"] >= previous["low"]:
            return False
            
        recent_lows = lookback_candles["low"].tail(5)
        if len(recent_lows) == 0:
            return False
            
        atr_val = self._calculate_atr(lookback_candles)
        threshold = atr_val * 0.1 if atr_val else current["low"] * 0.001
        
        for low_val in recent_lows:
            if abs(current["low"] - low_val) < threshold:
                return True
        return False

    def _is_stop_run_sweep(self, current_candle, df: pd.DataFrame) -> Dict:
        """Detect stop-run above/below swings"""
        if len(df) < 10:
            return {"valid": False}
            
        swing_highs = self._find_swing_highs(df.tail(20))
        swing_lows = self._find_swing_lows(df.tail(20))
        
        for swing_high in swing_highs[-3:]:
            if (current_candle["high"] > swing_high and 
                current_candle["close"] < swing_high and
                current_candle["low"] < swing_high):
                return {"valid": True, "type": "stop_run_high", "direction": "bearish"}
        
        for swing_low in swing_lows[-3:]:
            if (current_candle["low"] < swing_low and 
                current_candle["close"] > swing_low and
                current_candle["high"] > swing_low):
                return {"valid": True, "type": "stop_run_low", "direction": "bullish"}
        
        return {"valid": False}

    def _check_displacement(self, df: pd.DataFrame, sweep_result: Dict) -> Dict:
        """STEP 2: Check for strong displacement after sweep"""
        sweep_idx = sweep_result.get("sweep_index", -5)
        start_idx = max(0, len(df) + sweep_idx + 1)
        post_sweep_candles = df.iloc[start_idx:start_idx + 4]
        
        if len(post_sweep_candles) == 0:
            return {"valid": False, "reason": "No candles after sweep"}
        
        impulse_candle = None
        for i, candle in post_sweep_candles.iterrows():
            body_size = abs(candle["close"] - candle["open"])
            full_range = candle["high"] - candle["low"]
            
            if full_range > 0 and (body_size / full_range) >= 0.6:
                impulse_candle = candle
                break
        
        if impulse_candle is None:
            return {"valid": False, "reason": "No impulse candle (body < 60%)"}
        
        direction = sweep_result["direction"]
        is_bullish_impulse = impulse_candle["close"] > impulse_candle["open"]
        
        if direction == "bullish" and not is_bullish_impulse:
            return {"valid": False, "reason": "Bearish impulse after bullish sweep"}
            
        if direction == "bearish" and is_bullish_impulse:
            return {"valid": False, "reason": "Bullish impulse after bearish sweep"}
        
        return {
            "valid": True, 
            "impulse_candle": impulse_candle,
            "direction": direction,
            "displacement_index": df.index.get_loc(impulse_candle.name) if hasattr(impulse_candle, 'name') else -1
        }

    def _check_retracement_zone(self, df: pd.DataFrame, displacement_result: Dict, context: Dict) -> Dict:
        """STEP 3: Check retracement into valid mitigation zone"""
        current_price = df["close"].iloc[-1]
        direction = displacement_result["direction"]
        
        fvg_zone = self._find_fvg_zone(df, direction)
        if fvg_zone and self._price_in_zone(current_price, fvg_zone):
            return {
                "valid": True, 
                "zone_type": "fvg", 
                "zone": fvg_zone,
                "direction": direction
            }
        
        ob_zone = self._find_order_block(df, direction)
        if ob_zone and self._price_in_zone(current_price, ob_zone):
            return {
                "valid": True, 
                "zone_type": "order_block", 
                "zone": ob_zone,
                "direction": direction
            }
        
        imbalance_zone = self._find_imbalance_zone(df, direction)
        if imbalance_zone and self._price_in_zone(current_price, imbalance_zone):
            return {
                "valid": True, 
                "zone_type": "imbalance", 
                "zone": imbalance_zone,
                "direction": direction
            }
        
        return {"valid": False, "reason": "No valid mitigation zone touched"}

    def _check_premium_discount(self, df: pd.DataFrame, zone_result: Dict, context: Dict) -> Dict:
        """STEP 4: Premium/Discount filter using equilibrium"""
        current_price = df["close"].iloc[-1]
        direction = zone_result["direction"]
        
        swing_highs = self._find_swing_highs(df.tail(30))
        swing_lows = self._find_swing_lows(df.tail(30))
        
        if not swing_highs or not swing_lows:
            return {"valid": False, "reason": "Cannot determine equilibrium"}
            
        recent_swing_high = max(swing_highs[-3:])
        recent_swing_low = min(swing_lows[-3:])
        equilibrium = (recent_swing_high + recent_swing_low) / 2
        
        if direction == "bullish" and current_price > equilibrium:
            return {"valid": False, "reason": "Bullish entry in premium (above EQ)"}
            
        if direction == "bearish" and current_price < equilibrium:
            return {"valid": False, "reason": "Bearish entry in discount (below EQ)"}
        
        return {
            "valid": True,
            "equilibrium": equilibrium,
            "position": "discount" if direction == "bullish" else "premium",
            "direction": direction
        }

    def _check_htf_alignment(self, df: pd.DataFrame, equilibrium_result: Dict, context: Dict) -> Dict:
        """STEP 5: Higher Timeframe bias alignment"""
        direction = equilibrium_result["direction"]
        
        htf_data = context.get('df_15m') or context.get('df_1h')
        if htf_data is None or len(htf_data) < 20:
            return {"valid": False, "reason": "No HTF data available"}
        
        htf_trend = self._detect_htf_trend(htf_data)
        
        if direction == "bullish" and htf_trend != "bullish":
            return {"valid": False, "reason": "Bullish signal against HTF structure"}
            
        if direction == "bearish" and htf_trend != "bearish":
            return {"valid": False, "reason": "Bearish signal against HTF structure"}
        
        return {
            "valid": True,
            "htf_trend": htf_trend,
            "direction": direction
        }

    def _check_momentum_volatility(self, df: pd.DataFrame, htf_result: Dict, context: Dict) -> Dict:
        """STEP 6: Momentum & Volatility confirmation"""
        direction = htf_result["direction"]
        current_price = df["close"].iloc[-1]
        
        if len(df) >= 3:
            prev_candle = df.iloc[-2]
            current_candle = df.iloc[-1]
            
            if direction == "bullish":
                if not (current_candle["close"] > current_candle["open"] and 
                       current_candle["close"] > prev_candle["close"]):
                    return {"valid": False, "reason": "No bullish momentum confirmation"}
            else:
                if not (current_candle["close"] < current_candle["open"] and 
                       current_candle["close"] < prev_candle["close"]):
                    return {"valid": False, "reason": "No bearish momentum confirmation"}
        
        atr_val = self._calculate_atr(df.tail(14))
        if atr_val and atr_val < current_price * 0.001:
            return {"valid": False, "reason": "Volatility too low (no momentum)"}
        
        return {
            "valid": True,
            "momentum_confirmed": True,
            "volatility_acceptable": True,
            "direction": direction
        }

    def _format_rome_signal(self, final_result: Dict, symbol: str, context: Dict, df: pd.DataFrame) -> Dict:
        """Format valid Rome signal with OLD scoring"""
        direction = final_result["direction"]
        side = "BUY" if direction == "bullish" else "SELL"
        current_price = context.get('current_price', 0)
        tf = context.get('tf', '15m')
        
        # 🚀 UNIFIED SMART TP/SL SYSTEM
        sl, tp1, tp2, tp3 = RomeOptTPSL.calculate_rome_tp_sl(
            df, symbol, side, current_price, context
        )
        
        base_score = 6
        final_score = base_score + 5
        
        return {
            "symbol": symbol,
            "side": side,
            "entry": current_price,
            "sl": sl,
            "tp1": tp1,
            "tp2": tp2,
            "tp3": tp3,
            "score": base_score,
            "reason": "ROMEOPT Institutional Sequence",
            "reason_list": ["Rome Liquidity Sweep", "Rome Displacement", "Rome Zone Retrace", 
                          "Rome Premium/Discount", "Rome HTF Alignment", "Rome Momentum"],
            "timeframe": tf,
            "rome_sequence": True,
            "final_score_rome": final_score
        }

    # ==================== SUPPORTING DETECTION METHODS ====================

    def _calculate_atr(self, df: pd.DataFrame, period: int = 14) -> float:
        """Calculate Average True Range"""
        if len(df) < period:
            return 0.0
            
        high, low, close = df["high"], df["low"], df["close"]
        tr1 = high - low
        tr2 = (high - close.shift(1)).abs()
        tr3 = (low - close.shift(1)).abs()
        true_range = pd.DataFrame({'tr1': tr1, 'tr2': tr2, 'tr3': tr3}).max(axis=1)
        return true_range.rolling(period).mean().iloc[-1]

    def _find_swing_highs(self, df: pd.DataFrame, lookback: int = 3) -> List[float]:
        """Find swing highs in dataframe"""
        if len(df) < lookback * 2 + 1:
            return []
            
        highs = []
        for i in range(lookback, len(df) - lookback):
            if (df["high"].iloc[i] == df["high"].iloc[i-lookback:i+lookback+1].max()):
                highs.append(df["high"].iloc[i])
        return highs

    def _find_swing_lows(self, df: pd.DataFrame, lookback: int = 3) -> List[float]:
        """Find swing lows in dataframe"""
        if len(df) < lookback * 2 + 1:
            return []
            
        lows = []
        for i in range(lookback, len(df) - lookback):
            if (df["low"].iloc[i] == df["low"].iloc[i-lookback:i+lookback+1].min()):
                lows.append(df["low"].iloc[i])
        return lows

    def _find_fvg_zone(self, df: pd.DataFrame, direction: str) -> Optional[Dict]:
        """Find Fair Value Gap zones"""
        if len(df) < 3:
            return None
            
        for i in range(len(df) - 3, max(0, len(df) - 10), -1):
            if i + 2 >= len(df):
                continue
                
            c1, c2, c3 = df.iloc[i], df.iloc[i+1], df.iloc[i+2]
            
            if direction == "bullish" and c2["low"] > c1["high"]:
                return {"low": c1["high"], "high": c2["low"], "type": "bullish_fvg"}
            elif direction == "bearish" and c2["high"] < c1["low"]:
                return {"low": c2["high"], "high": c1["low"], "type": "bearish_fvg"}
                
        return None

    def _find_order_block(self, df: pd.DataFrame, direction: str) -> Optional[Dict]:
        """Find Order Block zones"""
        if len(df) < 5:
            return None
            
        for i in range(len(df) - 5, max(0, len(df) - 20), -1):
            if i >= len(df):
                continue
                
            candle = df.iloc[i]
            body_size = abs(candle["close"] - candle["open"])
            full_range = candle["high"] - candle["low"]
            
            if body_size / full_range >= 0.6:
                if direction == "bullish" and candle["close"] > candle["open"]:
                    return {"low": candle["low"], "high": candle["open"], "type": "bullish_ob"}
                elif direction == "bearish" and candle["close"] < candle["open"]:
                    return {"low": candle["close"], "high": candle["high"], "type": "bearish_ob"}
                    
        return None

    def _find_imbalance_zone(self, df: pd.DataFrame, direction: str) -> Optional[Dict]:
        """Find Imbalance zones"""
        if len(df) < 10:
            return None
            
        recent_high = df["high"].tail(10).max()
        recent_low = df["low"].tail(10).min()
        current_price = df["close"].iloc[-1]
        
        if direction == "bullish" and current_price < (recent_high + recent_low) * 0.4:
            return {"low": recent_low, "high": (recent_high + recent_low) * 0.4, "type": "bullish_imbalance"}
        elif direction == "bearish" and current_price > (recent_high + recent_low) * 0.6:
            return {"low": (recent_high + recent_low) * 0.6, "high": recent_high, "type": "bearish_imbalance"}
            
        return None

    def _price_in_zone(self, price: float, zone: Dict) -> bool:
        """Check if price is within a zone"""
        return zone["low"] <= price <= zone["high"]

    def _detect_htf_trend(self, htf_df: pd.DataFrame) -> str:
        """Detect HTF trend using simple EMA crossover"""
        if len(htf_df) < 50:
            return "neutral"
            
        ema_20 = htf_df["close"].ewm(span=20).mean().iloc[-1]
        ema_50 = htf_df["close"].ewm(span=50).mean().iloc[-1]
        current_price = htf_df["close"].iloc[-1]
        
        if current_price > ema_20 and current_price > ema_50 and ema_20 > ema_50:
            return "bullish"
        elif current_price < ema_20 and current_price < ema_50 and ema_20 < ema_50:
            return "bearish"
        else:
            return "neutral"

# ==================== YOUR EXACT OLD WINNER FILTERS ====================

class OriginalWinnerFilters:
    """YOUR EXACT ORIGINAL FILTERS - UNCHANGED"""
    
    @staticmethod
    def get_btc_direction(btc_15m: pd.DataFrame, btc_1h: pd.DataFrame) -> str:
        """YOUR EXACT BTC DIRECTION DETECTION"""
        if btc_15m is None or btc_1h is None or btc_15m.empty or btc_1h.empty: 
            return "NEUTRAL"
        try:
            price = btc_15m['close'].iloc[-1]
            ema_1h_50 = btc_1h['close'].ewm(span=50).mean().iloc[-1]
            ema_15m_20 = btc_15m['close'].ewm(span=20).mean().iloc[-1]
            
            if price > ema_1h_50 and price > ema_15m_20: 
                return "BULLISH"
            elif price < ema_1h_50 and price < ema_15m_20: 
                return "BEARISH"
            else: 
                return "NEUTRAL"
        except Exception as e:
            logging.error(f"BTC direction error: {e}")
            return "NEUTRAL"

    @staticmethod
    def is_trade_allowed(signal_side: SignalSide, btc_direction: str) -> bool:
        """YOUR EXACT BTC ALIGNMENT FILTER"""
        if btc_direction == "BULLISH": 
            return signal_side == SignalSide.BUY
        elif btc_direction == "BEARISH": 
            return signal_side == SignalSide.SELL
        else: 
            return True

    @staticmethod
    def check_higher_tf_alignment(signal, higher_tf_data: pd.DataFrame) -> bool:
        """YOUR EXACT HIGHER TIMEFRAME ALIGNMENT"""
        if higher_tf_data is None or len(higher_tf_data) < 20:
            return False
        try:
            current_price = signal.get('entry', 0) if isinstance(signal, dict) else signal.entry_price
            higher_tf_ema_20 = higher_tf_data['close'].ewm(span=20).mean().iloc[-1]
            higher_tf_ema_50 = higher_tf_data['close'].ewm(span=50).mean().iloc[-1]
            
            signal_side = SignalSide(signal['side']) if isinstance(signal, dict) else signal.side
            
            if signal_side == SignalSide.BUY:
                return current_price > higher_tf_ema_20 and current_price > higher_tf_ema_50
            else:
                return current_price < higher_tf_ema_20 and current_price < higher_tf_ema_50
        except Exception as e:
            logging.error(f"Higher TF alignment error: {e}")
            return False

    @staticmethod
    def check_momentum_confirmation(df: pd.DataFrame, signal_direction: SignalSide) -> bool:
        """YOUR EXACT MOMENTUM CONFIRMATION"""
        if df is None or len(df) < 3: 
            return False
        try:
            current_candle = df.iloc[-1]
            prev_candle = df.iloc[-2]
            
            if signal_direction == SignalSide.BUY:
                return (current_candle['close'] > current_candle['open'] and 
                        current_candle['close'] > prev_candle['close'])
            else:
                return (current_candle['close'] < current_candle['open'] and
                        current_candle['close'] < prev_candle['close'])
        except Exception as e:
            logging.error(f"Momentum confirmation error: {e}")
            return False

    @staticmethod
    def check_entry_zone_quality(df: pd.DataFrame, signal_direction: SignalSide) -> bool:
        """YOUR EXACT ZONE QUALITY DETECTION"""
        if df is None or len(df) < 15: 
            return False
        try:
            recent_high = df['high'].tail(15).max()
            recent_low = df['low'].tail(15).min()
            current_price = df['close'].iloc[-1]
            
            if recent_high == recent_low: 
                return False
                
            range_position = (current_price - recent_low) / (recent_high - recent_low)
            
            if signal_direction == SignalSide.BUY:
                return range_position < 0.3
            else:
                return range_position > 0.7
        except Exception as e:
            logging.error(f"Zone quality error: {e}")
            return False

    @staticmethod
    def detect_choppy_market(df: pd.DataFrame) -> bool:
        """YOUR EXACT MARKET CONDITION FILTER"""
        if df is None or len(df) < 25: 
            return True
        try:
            high, low, close = df['high'], df['low'], df['close']
            tr1 = high - low
            tr2 = (high - close.shift(1)).abs()
            tr3 = (low - close.shift(1)).abs()
            true_range = pd.DataFrame({'tr1': tr1, 'tr2': tr2, 'tr3': tr3}).max(axis=1)
            atr = true_range.rolling(14).mean().iloc[-1]
            
            current_price = close.iloc[-1]
            price_range_pct = (df['high'].tail(20).max() - df['low'].tail(20).min()) / current_price
            
            return (atr < (current_price * 0.002) and price_range_pct < 0.02)
        except Exception as e:
            logging.error(f"Choppy market detection error: {e}")
            return True

# ==================== YOUR EXACT OLD SMC CORE LOGIC ====================

class OriginalSMCLogic:
    """YOUR EXACT ORIGINAL SMC LOGIC - NOW WITH UNIFIED TP/SL"""
    
    def __init__(self):
        self.rome_analyzer = RomeSMCAnalyzer()
    
    @staticmethod
    def detect_swing_points(df: pd.DataFrame):
        if df is None or len(df) < 5: 
            return None
        last = df.iloc[-1]; prev = df.iloc[-3:-1]
        swing_high = last["high"] > prev["high"].max()
        swing_low = last["low"] < prev["low"].min()
        return swing_high, swing_low

    @staticmethod
    def detect_active_range(df: pd.DataFrame, lookback=10):
        if df is None or len(df) < lookback:
            return 0, 0
        last = df.iloc[-lookback:]
        return last["high"].max(), last["low"].min()

    @staticmethod
    def detect_sweep(df: pd.DataFrame):
        if df is None or len(df) < 6: 
            return False, False
        last = df.iloc[-1]; prev = df.iloc[-5:-1]
        return last["high"] > prev["high"].max(), last["low"] < prev["low"].min()

    @staticmethod
    def detect_bos_mss(df: pd.DataFrame):
        hh, ll = OriginalSMCLogic.detect_sweep(df)
        return hh, ll

    @staticmethod
    def detect_fvg(df: pd.DataFrame):
        if df is None or len(df) < 3: 
            return False, False
        c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
        bull = c2["low"] > c1["high"] and c3["low"] > c2["high"]
        bear = c2["high"] < c1["low"] and c3["high"] < c2["low"]
        return bull, bear

    @staticmethod
    def detect_order_blocks(df: pd.DataFrame):
        if df is None or len(df) < 3: 
            return None, None, None
        candle = df.iloc[-3]
        if candle["close"] > candle["open"]:
            return "bullish", candle["open"], candle["low"]
        return "bearish", candle["high"], candle["open"]

    def generate_signal(self, df: pd.DataFrame, symbol: str, context=None):
        """UPDATED: USES UNIFIED ROMEOPT TP/SL FOR ALL SIGNALS"""
        if context is None: 
            context = {}
        
        if df is None or len(df) < 20:
            return None

        tf = context.get("tf", "15m")

        try:
            # FIRST TRY ROMEOPT SEQUENCING (HIGHER QUALITY)
            rome_signal = self.rome_analyzer.generate_signal(df, symbol, context)
            if rome_signal:
                logging.info(f"🏛️ ROMEOPT Signal: {symbol} {rome_signal['side']} | Score: {rome_signal['score']}")
                return rome_signal
            
            # FALLBACK TO ORIGINAL LOGIC
            last = df["close"].iloc[-1]

            ob_type, ob_hi, ob_lo = self.detect_order_blocks(df)
            if ob_type is None: 
                return None

            bull_fvg, bear_fvg = self.detect_fvg(df)
            sweep_h, sweep_l = self.detect_sweep(df)
            bos_hh, bos_ll = self.detect_bos_mss(df)

            if not (bos_hh or bos_ll): 
                return None

            score = 0
            reasons = []

            if ob_type == "bullish": 
                score += 2
                reasons.append("OB Bull +2")
            else: 
                score += 2
                reasons.append("OB Bear +2")

            if bull_fvg: 
                score += 2
                reasons.append("FVG Bull +2")
            elif bear_fvg: 
                score += 2
                reasons.append("FVG Bear +2")

            score += 2
            reasons.append("BOS +2")
            if sweep_h or sweep_l: 
                score += 1
                reasons.append("Sweep +1")
            else: 
                reasons.append("No Sweep +0")

            side = "BUY" if ob_type == "bullish" else "SELL"

            # 🚀 UNIFIED SMART TP/SL SYSTEM FOR ALL SIGNALS
            entry = float(last)
            sl, tp1, tp2, tp3 = RomeOptTPSL.calculate_rome_tp_sl(
                df, symbol, side, entry, context
            )

            return {
                "symbol": symbol,
                "side": side,
                "entry": entry,
                "sl": sl,
                "tp1": tp1,
                "tp2": tp2,
                "tp3": tp3,
                "score": score,
                "reason": "Set B SMC Signal + SMART TP/SL",
                "reason_list": reasons,
                "timeframe": tf,
                "rome_sequence": False
            }
        except Exception as e:
            logging.error(f"Signal generation error for {symbol}: {e}")
            return None

# ==================== ENHANCED DATA MODELS ====================

@dataclass
class TradingSignal:
    """Enhanced signal with OLD scoring + new tracking"""
    symbol: str
    side: SignalSide
    entry_price: float
    stop_loss: float
    take_profit_1: float
    take_profit_2: float
    take_profit_3: float
    timestamp: datetime.datetime
    timeframe: str
    base_score: int
    final_score: int
    filters_passed: List[str]
    rejection_reasons: List[str]
    winner_filters_passed: List[str]
    winner_filters_failed: List[str]
    signal_id: str
    rome_sequence: bool = False
    version: str = "3.1-ROMEOPT-UNIFIED"

# ==================== OLD-STYLE FILTER APPLICATION ====================

class OldFilterApplicator:
    """APPLIES FILTERS EXACTLY LIKE OLD CODE"""
    
    @staticmethod
    async def apply_old_filters(old_signal: Dict, df: pd.DataFrame, context: Dict, config: ScannerConfig) -> Tuple[bool, List[str], List[str], List[str]]:
        """EXACT OLD CODE FILTER LOGIC"""
        winner_filters_passed = []
        winner_filters_failed = []
        filters_failed_reasons = []
        
        signal_side = SignalSide.BUY if old_signal['side'] == 'BUY' else SignalSide.SELL
        tf = context.get('tf', '15m')
        
        filters_passed = True
        
        if config.REQUIRE_BTC_ALIGNMENT:
            btc_direction = context.get('btc_direction', 'NEUTRAL')
            if OriginalWinnerFilters.is_trade_allowed(signal_side, btc_direction):
                winner_filters_passed.append("BTC_ALIGNMENT")
            else:
                filters_passed = False
                filters_failed_reasons.append(f"BTC {btc_direction} misalignment")
                winner_filters_failed.append("BTC_MISALIGNMENT")
                logging.info(f"⏸️ OLD-STYLE Blocked: {signal_side.value} vs BTC {btc_direction}")
        
        if filters_passed and config.REQUIRE_HIGHER_TF_ALIGNMENT:
            higher_tf_data = context.get('df_15m')
            if OriginalWinnerFilters.check_higher_tf_alignment(old_signal, higher_tf_data):
                winner_filters_passed.append("HIGHER_TF_ALIGNMENT")
            else:
                filters_passed = False
                filters_failed_reasons.append("Higher TF misalignment")
                winner_filters_failed.append("HIGHER_TF_MISALIGNMENT")
                logging.info(f"⏸️ OLD-STYLE Blocked: Higher TF misalignment")
        
        if (filters_passed and config.REQUIRE_MOMENTUM_CONFIRMATION and 
            tf not in ["1m", "3m"]):
            if OriginalWinnerFilters.check_momentum_confirmation(df, signal_side):
                winner_filters_passed.append("MOMENTUM")
            else:
                filters_passed = False
                filters_failed_reasons.append("No momentum confirmation")
                winner_filters_failed.append("WEAK_MOMENTUM")
                logging.info(f"⏸️ OLD-STYLE Blocked: No momentum confirmation")
        
        if filters_passed and config.REQUIRE_ZONE_QUALITY:
            if OriginalWinnerFilters.check_entry_zone_quality(df, signal_side):
                winner_filters_passed.append("ZONE_QUALITY")
            else:
                filters_passed = False
                filters_failed_reasons.append("Poor entry zone")
                winner_filters_failed.append("POOR_ZONE")
                logging.info(f"⏸️ OLD-STYLE Blocked: Poor entry zone")
        
        if filters_passed and config.AVOID_CHOPPY_MARKETS:
            if not OriginalWinnerFilters.detect_choppy_market(df):
                winner_filters_passed.append("TRENDING_MARKET")
            else:
                filters_passed = False
                filters_failed_reasons.append("Choppy market")
                winner_filters_failed.append("CHOPPY_MARKET")
                logging.info(f"⏸️ OLD-STYLE Blocked: Choppy market")
        
        return filters_passed, winner_filters_passed, winner_filters_failed, filters_failed_reasons

# ==================== TRADE MONITORING SYSTEM ====================

class TradeMonitor:
    """Advanced monitoring with OLD signal handling"""
    
    def __init__(self, scanner):
        self.scanner = scanner
        self.open_signals = {}
        self.closed_trades = []
        self.all_signals = []
        self.last_summary_time = time.time()
        self.recent_sl = defaultdict(lambda: deque())
        
    async def add_signal(self, signal: TradingSignal):
        """Add OLD-style signal to monitoring"""
        self.open_signals[signal.signal_id] = signal
        self.all_signals.append({
            'signal': signal,
            'status': 'OPEN',
            'added_time': datetime.datetime.utcnow()
        })
        rome_tag = " 🏛️" if signal.rome_sequence else ""
        logging.info(f"📈 OLD Monitoring: {signal.symbol} {signal.side.value} | Final Score: {signal.final_score}{rome_tag}")
        
    def record_sl_hit(self, symbol: str, lookback_minutes=30):
        """YOUR EXACT OLD SL-CLUSTER LOGIC"""
        now = time.time()
        dq = self.recent_sl[symbol]
        dq.append(now)
        cutoff = now - lookback_minutes * 60
        while dq and dq[0] < cutoff: 
            dq.popleft()
        
    def deprioritized(self, symbol: str, threshold=3, lookback=30):
        """YOUR EXACT OLD DEPRIORITIZATION LOGIC"""
        dq = self.recent_sl[symbol]
        now = time.time()
        cutoff = now - lookback * 60
        while dq and dq[0] < cutoff: 
            dq.popleft()
        return len(dq) >= threshold

    async def monitor_open_signals(self):
        """Monitor OLD signals"""
        if not self.open_signals: 
            return
        
        signals_to_remove = []
        
        for signal_id, signal in self.open_signals.items():
            try:
                ticker = await self.scanner.exchange.fetch_ticker(signal.symbol)
                current_price = ticker['last']
                
                status = await self.check_signal_status(signal, current_price)
                
                if status != "OPEN":
                    await self._process_closed_signal(signal, status, current_price)
                    signals_to_remove.append(signal_id)
                    if "SL" in status:
                        self.record_sl_hit(signal.symbol)
                    
            except Exception as e:
                logging.error(f"Error monitoring {signal.symbol}: {e}")
        
        for signal_id in signals_to_remove:
            if signal_id in self.open_signals:
                del self.open_signals[signal_id]

    async def check_signal_status(self, signal: TradingSignal, current_price: float):
        """Check TP/SL hits for OLD signals"""
        try:
            if signal.side == SignalSide.BUY:
                if current_price >= signal.take_profit_3: 
                    return "TP3_HIT"
                elif current_price >= signal.take_profit_2: 
                    return "TP2_HIT"
                elif current_price >= signal.take_profit_1: 
                    return "TP1_HIT"
                elif current_price <= signal.stop_loss: 
                    return "SL_HIT"
            else:
                if current_price <= signal.take_profit_3: 
                    return "TP3_HIT"
                elif current_price <= signal.take_profit_2: 
                    return "TP2_HIT"
                elif current_price <= signal.take_profit_1: 
                    return "TP1_HIT"
                elif current_price >= signal.stop_loss: 
                    return "SL_HIT"
            return "OPEN"
        except Exception as e:
            logging.error(f"Signal status check error: {e}")
            return "OPEN"

    async def _process_closed_signal(self, signal: TradingSignal, status: str, close_price: float):
        """Process closed OLD signal"""
        try:
            if signal.side == SignalSide.BUY:
                pnl_pct = (close_price - signal.entry_price) / signal.entry_price * 100
            else:
                pnl_pct = (signal.entry_price - close_price) / signal.entry_price * 100
            
            trade_record = {
                'signal_id': signal.signal_id,
                'symbol': signal.symbol,
                'side': signal.side.value,
                'entry_price': signal.entry_price,
                'close_price': close_price,
                'pnl_pct': pnl_pct,
                'status': status,
                'entry_time': signal.timestamp,
                'exit_time': datetime.datetime.utcnow(),
                'timeframe': signal.timeframe,
                'final_score': signal.final_score,
                'winner_filters_passed': signal.winner_filters_passed,
                'rome_sequence': signal.rome_sequence
            }
            
            self.closed_trades.append(trade_record)
            
            for sig_data in self.all_signals:
                if sig_data['signal'].signal_id == signal.signal_id:
                    sig_data['status'] = status
                    sig_data['close_price'] = close_price
                    sig_data['pnl_pct'] = pnl_pct
                    sig_data['exit_time'] = datetime.datetime.utcnow()
                    break
            
            await self._send_trade_update(signal, status, close_price, pnl_pct)
            rome_tag = " 🏛️" if signal.rome_sequence else ""
            logging.info(f"🎯 OLD Trade closed: {signal.symbol} {status} | P&L: {pnl_pct:.2f}% | Score: {signal.final_score}{rome_tag}")
        except Exception as e:
            logging.error(f"Process closed signal error: {e}")

    async def _send_trade_update(self, signal: TradingSignal, status: str, close_price: float, pnl_pct: float):
        """Send OLD-style trade update"""
        try:
            emoji = "🟢" if "TP" in status else "🔴"
            winner_info = f"✅ Filters: {', '.join(signal.winner_filters_passed)}\n" if signal.winner_filters_passed else ""
            rome_tag = "🏛️ ROMEOPT" if signal.rome_sequence else "OLD-STYLE"
            
            message = f"""
{emoji} **{rome_tag} TRADE UPDATE** {emoji}

Symbol: {signal.symbol}
Side: {signal.side.value}
Status: {status}

Entry: {signal.entry_price:.6f}
Exit: {close_price:.6f}
P&L: {pnl_pct:+.2f}%

{winner_info}
OLD Final Score: {signal.final_score}
"""
            await send_telegram_message(message)
        except Exception as e:
            logging.error(f"Send trade update error: {e}")

    async def send_performance_summary(self):
        """Send 2-hour performance summary"""
        try:
            now = time.time()
            if now - self.last_summary_time < 7200:
                return False
                
            self.last_summary_time = now
            
            two_hours_ago = datetime.datetime.utcnow() - datetime.timedelta(hours=2)
            recent_signals = [s for s in self.all_signals if s['added_time'] >= two_hours_ago]
            
            if not recent_signals:
                return True
                
            open_signals = [s for s in recent_signals if s['status'] == 'OPEN']
            closed_signals = [s for s in recent_signals if s['status'] != 'OPEN']
            winning_trades = [s for s in closed_signals if s.get('pnl_pct', 0) > 0]
            rome_signals = [s for s in recent_signals if s['signal'].rome_sequence]
            
            total_signals = len(recent_signals)
            win_rate = len(winning_trades) / len(closed_signals) * 100 if closed_signals else 0
            avg_final_score = sum(s['signal'].final_score for s in recent_signals) / total_signals if total_signals else 0

            message = f"""
📊 **ROMEOPT 2-HOUR PERFORMANCE** 📊

⏰ Period: Last 2 hours
📈 Total Signals: {total_signals}
🏛️ Rome Signals: {len(rome_signals)}
🟢 Open Signals: {len(open_signals)}
🔒 Closed Signals: {len(closed_signals)}
🎯 Win Rate: {win_rate:.1f}%
⭐ Avg Final Score: {avg_final_score:.1f}

📋 **RECENT SIGNALS:**
"""
            
            for i, sig_data in enumerate(recent_signals[-5:], 1):
                signal = sig_data['signal']
                status = sig_data['status']
                pnl = sig_data.get('pnl_pct', 0)
                
                status_emoji = "🟢" if "TP" in status else "🔴" if status == "SL_HIT" else "🟡"
                pnl_str = f"{pnl:+.2f}%" if status != "OPEN" else "OPEN"
                rome_emoji = "🏛️" if signal.rome_sequence else "🔹"
                
                winner_info = f" ✅{len(signal.winner_filters_passed)}" if signal.winner_filters_passed else ""
                
                message += f"{i}. {status_emoji} {rome_emoji} {signal.symbol} {signal.side.value} | Final: {signal.final_score}{winner_info} | {pnl_str}\n"
            
            await send_telegram_message(message)
            logging.info("📊 ROMEOPT 2-hour performance summary sent")
            return True
        except Exception as e:
            logging.error(f"Performance summary error: {e}")
            return False

    def get_performance_stats(self):
        """Get OLD-style performance statistics"""
        try:
            if not self.closed_trades:
                return {"total_trades": 0, "win_rate": 0, "avg_pnl": 0, "total_pnl": 0}
            
            winning_trades = [t for t in self.closed_trades if t['pnl_pct'] > 0]
            total_pnl = sum(t['pnl_pct'] for t in self.closed_trades)
            rome_trades = [t for t in self.closed_trades if t.get('rome_sequence', False)]
            rome_winning = [t for t in rome_trades if t['pnl_pct'] > 0]
            
            stats = {
                'total_trades': len(self.closed_trades),
                'winning_trades': len(winning_trades),
                'win_rate': len(winning_trades) / len(self.closed_trades) * 100 if self.closed_trades else 0,
                'avg_pnl': total_pnl / len(self.closed_trades) if self.closed_trades else 0,
                'total_pnl': total_pnl,
                'rome_trades': len(rome_trades),
                'rome_win_rate': len(rome_winning) / len(rome_trades) * 100 if rome_trades else 0
            }
            return stats
        except Exception as e:
            logging.error(f"Performance stats error: {e}")
            return {"total_trades": 0, "win_rate": 0, "avg_pnl": 0, "total_pnl": 0}

# ==================== ULTIMATE HYBRID SCANNER ====================

class UltimateHybridScanner:
    """PERFECT FUSION: ROMEOPT SEQUENCING + OLD FILTERS + UNIFIED TP/SL"""
    
    def __init__(self, config: ScannerConfig):
        self.config = config
        self.trade_monitor = TradeMonitor(self)
        self.exchange = None
        self.signal_cooldown = {}
        
        self.winner_filters = OriginalWinnerFilters()
        self.smc_logic = OriginalSMCLogic()
        self.old_filters = OldFilterApplicator()
        
        self._setup_logging()
    
    def _setup_logging(self):
        """Enhanced logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s | %(levelname)s | %(message)s',
            handlers=[
                logging.StreamHandler()
            ]
        )
        logging.info("🚀 ULTIMATE HYBRID SCANNER v3.1 - UNIFIED SMART TP/SL INITIALIZED")
        logging.info("✅ Strict 6-step RomeOPT institutional sequencing")
        logging.info("✅ Your exact old filter strictness & scoring preserved")
        logging.info("✅ UNIFIED SMART TP/SL for ALL signals")

    async def initialize_exchange(self):
        """Initialize with your exchange settings"""
        try:
            self.exchange = ccxt.okx({
                "enableRateLimit": True,
            })
            await self.exchange.load_markets()
            logging.info("✅ OKX exchange initialized successfully")
            return True
        except Exception as e:
            logging.error(f"❌ Exchange initialization failed: {e}")
            return False

    async def fetch_ohlcv_data(self, symbol: str, timeframe: str, limit: int = 200) -> Optional[pd.DataFrame]:
        """YOUR EXACT OHLCV FETCHING"""
        try:
            ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            if not ohlcv or len(ohlcv) < 20: 
                return None
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            return df
        except Exception as e:
            logging.debug(f"Could not fetch {symbol} {timeframe}: {e}")
            return None

    async def get_btc_context(self) -> Dict[str, Any]:
        """YOUR EXACT BTC CONTEXT"""
        try:
            btc_15m = await self.fetch_ohlcv_data('BTC/USDT', '15m', 100)
            btc_1h = await self.fetch_ohlcv_data('BTC/USDT', '1h', 100)
            btc_direction = self.winner_filters.get_btc_direction(btc_15m, btc_1h)
            return {
                'btc_direction': btc_direction,
                'df_15m': btc_15m,
                'df_1h': btc_1h
            }
        except Exception as e:
            logging.error(f"Error getting BTC context: {e}")
            return {'btc_direction': 'NEUTRAL'}

    async def scan_symbol(self, symbol: str) -> List[TradingSignal]:
        """ROMEOPT SCANNING WITH OLD FILTER STRICTNESS"""
        signals = []
        
        try:
            context = await self.get_btc_context()
            
            timeframes = ["1m", "3m", "5m", "15m", "30m"]
            
            for tf in timeframes:
                cooldown_key = f"{symbol}_{tf}"
                if cooldown_key in self.signal_cooldown:
                    if time.time() - self.signal_cooldown[cooldown_key] < self.config.COOLDOWN_MINUTES * 60:
                        continue
                
                if self.trade_monitor.deprioritized(symbol):
                    continue
                
                df = await self.fetch_ohlcv_data(symbol, tf)
                if df is None or len(df) < 20:
                    continue
                
                scan_context = context.copy()
                scan_context['tf'] = tf
                scan_context['current_price'] = df['close'].iloc[-1]
                
                if tf in ["1m", "3m", "5m"]:
                    df_15m = await self.fetch_ohlcv_data(symbol, '15m', 100)
                    df_1h = await self.fetch_ohlcv_data(symbol, '1h', 100)
                    scan_context['df_15m'] = df_15m
                    scan_context['df_1h'] = df_1h
                
                old_signal = self.smc_logic.generate_signal(df, symbol, scan_context)
                if not old_signal: 
                    continue
                
                hybrid_signal = await self._apply_old_style_filters(old_signal, df, scan_context)
                if hybrid_signal:
                    if await self._validate_signal(hybrid_signal):
                        signals.append(hybrid_signal)
                        self.signal_cooldown[cooldown_key] = time.time()
                        await self.trade_monitor.add_signal(hybrid_signal)
                        
                        await self._send_signal_notification(hybrid_signal, old_signal)
                        
                        rome_tag = " 🏛️" if hybrid_signal.rome_sequence else ""
                        logging.info(f"🏆 UNIFIED SIGNAL: {symbol} {hybrid_signal.side.value} "
                                   f"| Base: {hybrid_signal.base_score} "
                                   f"| Final: {hybrid_signal.final_score}{rome_tag}")
        
        except Exception as e:
            logging.error(f"Error scanning {symbol}: {e}")
            
        return signals

    async def _apply_old_style_filters(self, old_signal: Dict, df: pd.DataFrame, context: Dict) -> Optional[TradingSignal]:
        """APPLY FILTERS EXACTLY LIKE OLD CODE"""
        try:
            filters_passed, winner_filters_passed, winner_filters_failed, filter_reasons = (
                await self.old_filters.apply_old_filters(old_signal, df, context, self.config)
            )
            
            if not filters_passed:
                return None
            
            base_score = old_signal['score']
            final_score = base_score + self.config.WINNER_BONUS
            
            rome_sequence = old_signal.get('rome_sequence', False)
            
            enhanced_signal = TradingSignal(
                symbol=old_signal['symbol'],
                side=SignalSide.BUY if old_signal['side'] == 'BUY' else SignalSide.SELL,
                entry_price=old_signal['entry'],
                stop_loss=old_signal['sl'],
                take_profit_1=old_signal['tp1'],
                take_profit_2=old_signal['tp2'],
                take_profit_3=old_signal['tp3'],
                timestamp=datetime.datetime.utcnow(),
                timeframe=old_signal['timeframe'],
                base_score=base_score,
                final_score=final_score,
                filters_passed=old_signal['reason_list'],
                rejection_reasons=filter_reasons,
                winner_filters_passed=winner_filters_passed,
                winner_filters_failed=winner_filters_failed,
                signal_id=f"{old_signal['symbol']}_{old_signal['timeframe']}_{int(time.time())}",
                rome_sequence=rome_sequence
            )
            
            rome_tag = " 🏛️" if rome_sequence else ""
            logging.info(f"✅ OLD FILTERS PASSED: {len(winner_filters_passed)} - Score: {base_score} + {self.config.WINNER_BONUS} = {final_score}{rome_tag}")
            return enhanced_signal
        except Exception as e:
            logging.error(f"Apply old filters error: {e}")
            return None

    async def _send_signal_notification(self, hybrid_signal: TradingSignal, old_signal: Dict):
        """Send signal notification"""
        try:
            rome_tag = "🏛️ ROMEOPT INSTITUTIONAL" if hybrid_signal.rome_sequence else "OLD-STYLE"
            rome_emoji = "🏛️" if hybrid_signal.rome_sequence else "🔹"
            
            message = f"""
{rome_emoji} **{rome_tag} SIGNAL** {rome_emoji}

Symbol: {hybrid_signal.symbol}
Side: {hybrid_signal.side.value}
Timeframe: {hybrid_signal.timeframe}
Entry: {hybrid_signal.entry_price:.6f}

Risk Management:
SL: {hybrid_signal.stop_loss:.6f}
TP1: {hybrid_signal.take_profit_1:.6f}
TP2: {hybrid_signal.take_profit_2:.6f}
TP3: {hybrid_signal.take_profit_3:.6f}

OLD SCORING:
Base SMC: {hybrid_signal.base_score}
Winner Bonus: +{self.config.WINNER_BONUS}
FINAL SCORE: {hybrid_signal.final_score}

Filters: {', '.join(hybrid_signal.winner_filters_passed)}
"""
            
            if hybrid_signal.rome_sequence:
                message += f"Rome Sequence: {', '.join(hybrid_signal.filters_passed)}\n"
            else:
                message += f"Signal Reasons: {', '.join(old_signal['reason_list'])}\n"
        
            await send_telegram_message(message)
        except Exception as e:
            logging.error(f"Send signal notification error: {e}")

    async def _validate_signal(self, signal: TradingSignal) -> bool:
        """Final validation"""
        try:
            for open_signal in self.trade_monitor.open_signals.values():
                if open_signal.symbol == signal.symbol:
                    logging.info(f"⏸️ Already monitoring {signal.symbol}")
                    return False
                    
            return True
        except Exception as e:
            logging.error(f"Signal validation error: {e}")
            return False

    async def get_top_symbols(self) -> List[str]:
        """Get top symbols with your filters"""
        try:
            tickers = await self.exchange.fetch_tickers()
            symbols_data = []
            
            for symbol, ticker in tickers.items():
                if not symbol.endswith('/USDT'): 
                    continue
                
                volume_usdt = ticker.get('baseVolume', 0) * ticker.get('last', 0)
                if volume_usdt < self.config.MIN_VOLUME_USDT: 
                    continue
                
                bid = ticker.get('bid', 0)
                ask = ticker.get('ask', 0)
                if bid == 0 or ask == 0: 
                    continue
                
                spread_pct = (ask - bid) / bid
                if spread_pct > self.config.MAX_SPREAD_PCT: 
                    continue
                
                symbols_data.append({'symbol': symbol, 'volume': volume_usdt})
                    
            symbols_data.sort(key=lambda x: x['volume'], reverse=True)
            top_symbols = [s['symbol'] for s in symbols_data[:self.config.TOP_N_SYMBOLS]]
            
            logging.info(f"📊 Selected {len(top_symbols)} elite symbols")
            return top_symbols
            
        except Exception as e:
            logging.error(f"Error getting top symbols: {e}")
            return []

    async def run_scan_cycle(self):
        """Enhanced scanning with performance tracking"""
        try:
            logging.info("🔍 Starting UNIFIED scan cycle...")
            
            symbols = await self.get_top_symbols()
            if not symbols:
                logging.warning("No symbols to scan")
                return
                
            all_signals = []
            
            for symbol in symbols:
                try:
                    signals = await self.scan_symbol(symbol)
                    all_signals.extend(signals)
                    await asyncio.sleep(0.1)
                except Exception as e:
                    logging.error(f"Error scanning {symbol}: {e}")
                    continue
            
            rome_signals = [s for s in all_signals if s.rome_sequence]
            if all_signals:
                logging.info(f"📈 UNIFIED scan complete: {len(all_signals)} signals ({len(rome_signals)} Rome) found")
            else:
                logging.info("📈 UNIFIED scan complete: No signals found")
                
        except Exception as e:
            logging.error(f"UNIFIED scan cycle error: {e}")

    async def start_continuous_scanning(self):
        """Ultimate continuous scanning"""
        logging.info("🔄 Starting UNIFIED continuous scanning...")
        
        startup_msg = (
            "🚀 **UNIFIED SMART TP/SL SCANNER STARTED** 🚀\n"
            "✅ Strict 6-step RomeOPT institutional sequencing\n"
            "✅ Your exact old filters & scoring preserved\n"
            "✅ UNIFIED SMART TP/SL for ALL signals\n"
            "✅ Liquidity-based targets & story-based stops\n"
            "🎯 Target: INSTITUTIONAL-GRADE SIGNALS"
        )
        await send_telegram_message(startup_msg)
        
        try:
            while True:
                start_time = time.time()
                
                await self.run_scan_cycle()
                await self.trade_monitor.monitor_open_signals()
                await self.trade_monitor.send_performance_summary()
                
                elapsed = time.time() - start_time
                sleep_time = max(1, self.config.SCAN_INTERVAL - elapsed)
                await asyncio.sleep(sleep_time)
                
        except Exception as e:
            logging.error(f"UNIFIED scanning error: {e}")
            await asyncio.sleep(60)

    async def cleanup(self):
        """Cleanup resources"""
        try:
            if self.exchange:
                await self.exchange.close()
            logging.info("🧹 UNIFIED scanner cleanup completed")
        except Exception as e:
            logging.error(f"Cleanup error: {e}")

# ==================== TELEGRAM NOTIFICATIONS ====================

async def send_telegram_message(message: str):
    """Your exact Telegram function"""
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    if not token or not chat_id: 
        print(f"📱 TELEGRAM: {message}")
        return
        
    def escape_html(msg: str) -> str:
        if not msg: 
            return "-"
        return str(msg).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    
    safe_msg = escape_html(message)
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    async with httpx.AsyncClient() as client:
        try:
            await client.post(url, json={
                "chat_id": chat_id, 
                "text": safe_msg, 
                "parse_mode": "HTML"
            })
        except Exception as e:
            logging.error(f"Telegram failed: {e}")

# ==================== WEB API SERVER ====================

scanner: Optional[UltimateHybridScanner] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage scanner lifecycle"""
    global scanner
    config = ScannerConfig()
    scanner = UltimateHybridScanner(config)
    success = await scanner.initialize_exchange()
    
    if success:
        background_tasks = BackgroundTasks()
        background_tasks.add_task(scanner.start_continuous_scanning)
    
    yield
    
    if scanner:
        await scanner.cleanup()

app = FastAPI(title="Ultimate Hybrid Scanner v3.1 - UNIFIED", version="3.1.0", lifespan=lifespan)

class SignalResponse(BaseModel):
    symbol: str
    side: str
    entry_price: float
    base_score: int
    final_score: int
    timeframe: str
    winner_filters_passed: List[str]
    rome_sequence: bool
    timestamp: datetime.datetime

class PerformanceStats(BaseModel):
    total_trades: int
    win_rate: float
    avg_pnl: float
    open_signals: int
    rome_trades: int
    rome_win_rate: float

@app.get("/")
async def root():
    return {"status": "ULTIMATE HYBRID SCANNER v3.1 - UNIFIED SMART TP/SL - RUNNING"}

@app.get("/signals", response_model=List[SignalResponse])
async def get_current_signals():
    if not scanner: 
        return []
    signals = []
    for signal in scanner.trade_monitor.open_signals.values():
        signals.append(SignalResponse(
            symbol=signal.symbol,
            side=signal.side.value,
            entry_price=signal.entry_price,
            base_score=signal.base_score,
            final_score=signal.final_score,
            timeframe=signal.timeframe,
            winner_filters_passed=signal.winner_filters_passed,
            rome_sequence=signal.rome_sequence,
            timestamp=signal.timestamp
        ))
    return signals

@app.get("/performance", response_model=PerformanceStats)
async def get_performance():
    if not scanner:
        return PerformanceStats(total_trades=0, win_rate=0, avg_pnl=0, open_signals=0, rome_trades=0, rome_win_rate=0)
    stats = scanner.trade_monitor.get_performance_stats()
    return PerformanceStats(
        total_trades=stats['total_trades'],
        win_rate=stats['win_rate'],
        avg_pnl=stats['avg_pnl'],
        open_signals=len(scanner.trade_monitor.open_signals),
        rome_trades=stats.get('rome_trades', 0),
        rome_win_rate=stats.get('rome_win_rate', 0)
    )

@app.post("/scan-now")
async def trigger_manual_scan():
    """Trigger manual scan cycle"""
    if not scanner:
        raise HTTPException(status_code=500, detail="Scanner not initialized")
    
    asyncio.create_task(scanner.run_scan_cycle())
    return {"status": "UNIFIED scan triggered"}

# ==================== MAIN EXECUTION ====================

async def main():
    """Ultimate main execution with unified TP/SL"""
    try:
        config = ScannerConfig()
        scanner = UltimateHybridScanner(config)
        success = await scanner.initialize_exchange()
        
        if not success:
            logging.error("❌ Failed to initialize exchange. Exiting.")
            return
        
        await scanner.start_continuous_scanning()
        
    except KeyboardInterrupt:
        logging.info("🛑 UNIFIED scanner stopped by user")
    except Exception as e:
        logging.error(f"❌ UNIFIED scanner error: {e}")
    finally:
        if 'scanner' in locals():
            await scanner.cleanup()

if __name__ == "__main__":
    asyncio.run(main())