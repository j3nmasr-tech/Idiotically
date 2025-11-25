#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🏛️ PURE ROMEOPT INSTITUTIONAL SEQUENCING ENGINE 🏛️
- STRICT 6-STEP ROMEOPT SEQUENCE ONLY
- ALL TIMEFRAMES: 1m, 3m, 5m, 15m, 30m, 1h
- NO ADDITIONAL FILTERS - PURE INSTITUTIONAL LOGIC
- ALL MONITORING, TRACKING & 2-HOUR SUMMARIES PRESERVED
- BINGX API INTEGRATION - FULLY WORKING
- FIXED TP/SL TRACKING
- 🚨 FIXED PRICE CONSISTENCY ISSUE - NO MORE LATE SIGNALS
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
import pandas as pd
import numpy as np
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from pydantic import BaseModel
import uvicorn
from collections import defaultdict, deque
import json
from contextlib import asynccontextmanager
import hmac
import hashlib
import urllib.parse

# ==================== BINGX API CONFIGURATION ====================

class BingXAPI:
    """BingX API integration for data fetching"""
    
    def __init__(self, api_key: str = None, secret_key: str = None):
        self.api_key = api_key or os.getenv('BINGX_API_KEY', '')
        self.secret_key = secret_key or os.getenv('BINGX_SECRET_KEY', '')
        self.base_url = "https://open-api.bingx.com"
    
    def _get_timestamp(self) -> int:
        return int(time.time() * 1000)
    
    def _format_symbol(self, symbol: str) -> str:
        if '/' in symbol:
            return symbol.replace('/', '-')
        return symbol
    
    def _safe_float_convert(self, value) -> float:
        try:
            if isinstance(value, str):
                cleaned = value.replace('%', '').replace(',', '').strip()
                return float(cleaned)
            return float(value)
        except (ValueError, TypeError):
            return 0.0
    
    async def fetch_ohlcv(self, symbol: str, timeframe: str = '15m', limit: int = 200) -> Optional[List]:
        try:
            tf_mapping = {
                '1m': '1m', '3m': '3m', '5m': '5m', 
                '15m': '15m', '30m': '30m', '1h': '1h',
                '4h': '4h', '1d': '1d', '1w': '1w'
            }
            bingx_tf = tf_mapping.get(timeframe, '15m')
            
            endpoint = "/openApi/spot/v1/market/kline"
            formatted_symbol = self._format_symbol(symbol)
            
            params = {
                'symbol': formatted_symbol,
                'interval': bingx_tf,
                'limit': limit,
                'timestamp': self._get_timestamp()
            }
            
            url = f"{self.base_url}{endpoint}"
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                
                if data.get('code') == 0 and 'data' in data:
                    ohlcv_data = []
                    for candle in data['data']:
                        ohlcv_data.append([
                            candle[0],
                            self._safe_float_convert(candle[1]),
                            self._safe_float_convert(candle[2]),
                            self._safe_float_convert(candle[3]),
                            self._safe_float_convert(candle[4]),
                            self._safe_float_convert(candle[5])
                        ])
                    return ohlcv_data
                else:
                    logging.error(f"BingX OHLCV API error: {data}")
                    return None
                    
        except Exception as e:
            logging.error(f"BingX OHLCV fetch error for {symbol}: {e}")
            return None
    
    async def fetch_ticker(self, symbol: str) -> Optional[Dict]:
        try:
            endpoint = "/openApi/spot/v1/ticker/24hr"
            formatted_symbol = self._format_symbol(symbol)
            
            params = {
                'symbol': formatted_symbol,
                'timestamp': self._get_timestamp()
            }
            
            url = f"{self.base_url}{endpoint}"
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                
                if data.get('code') == 0 and 'data' in data:
                    ticker_data = data['data']
                    return {
                        'symbol': symbol,
                        'last': self._safe_float_convert(ticker_data.get('lastPrice', 0)),
                        'bid': self._safe_float_convert(ticker_data.get('bidPrice', 0)),
                        'ask': self._safe_float_convert(ticker_data.get('askPrice', 0)),
                        'high': self._safe_float_convert(ticker_data.get('highPrice', 0)),
                        'low': self._safe_float_convert(ticker_data.get('lowPrice', 0)),
                        'volume': self._safe_float_convert(ticker_data.get('volume', 0)),
                        'baseVolume': self._safe_float_convert(ticker_data.get('volume', 0)),
                        'quoteVolume': self._safe_float_convert(ticker_data.get('quoteVolume', 0)),
                        'change': self._safe_float_convert(ticker_data.get('priceChange', 0)),
                        'percentage': self._safe_float_convert(ticker_data.get('priceChangePercent', 0))
                    }
                else:
                    logging.error(f"BingX ticker API error: {data}")
                    return None
                    
        except Exception as e:
            logging.error(f"BingX ticker fetch error for {symbol}: {e}")
            return None
    
    async def fetch_tickers(self) -> Dict:
        try:
            endpoint = "/openApi/spot/v1/ticker/24hr"
            
            params = {
                'timestamp': self._get_timestamp()
            }
            
            url = f"{self.base_url}{endpoint}"
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                
                tickers = {}
                if data.get('code') == 0 and 'data' in data:
                    for ticker_data in data['data']:
                        symbol_str = ticker_data.get('symbol', '')
                        if '-' in symbol_str:
                            standard_symbol = symbol_str.replace('-', '/')
                            tickers[standard_symbol] = {
                                'symbol': standard_symbol,
                                'last': self._safe_float_convert(ticker_data.get('lastPrice', 0)),
                                'bid': self._safe_float_convert(ticker_data.get('bidPrice', 0)),
                                'ask': self._safe_float_convert(ticker_data.get('askPrice', 0)),
                                'high': self._safe_float_convert(ticker_data.get('highPrice', 0)),
                                'low': self._safe_float_convert(ticker_data.get('lowPrice', 0)),
                                'volume': self._safe_float_convert(ticker_data.get('volume', 0)),
                                'baseVolume': self._safe_float_convert(ticker_data.get('volume', 0)),
                                'quoteVolume': self._safe_float_convert(ticker_data.get('quoteVolume', 0)),
                                'change': self._safe_float_convert(ticker_data.get('priceChange', 0)),
                                'percentage': self._safe_float_convert(ticker_data.get('priceChangePercent', 0))
                            }
                    logging.info(f"✅ Successfully fetched {len(tickers)} tickers from BingX")
                    return tickers
                else:
                    logging.error(f"BingX tickers API error: {data}")
                    return {}
                
        except Exception as e:
            logging.error(f"BingX tickers fetch error: {e}")
            return {}
    
    async def test_connectivity(self) -> bool:
        try:
            logging.info("🔧 Testing BingX API connectivity...")
            tickers = await self.fetch_tickers()
            if tickers and len(tickers) > 0:
                logging.info(f"✅ Connectivity test passed: {len(tickers)} tickers received")
                return True
            else:
                logging.error("❌ Connectivity test failed: No tickers received")
                return False
            
        except Exception as e:
            logging.error(f"❌ Connectivity test error: {e}")
            return False

# ==================== PURE ROMEOPT CONFIGURATION ====================

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
class PureRomeConfig:
    # Core scanning settings
    SCAN_INTERVAL: int = 60
    TOP_N_SYMBOLS: int = 60
    MIN_VOLUME_USDT: float = 1000000
    MAX_SPREAD_PCT: float = 0.002
    
    # Pure RomeOPT sequencing only - no additional filters
    COOLDOWN_MINUTES: int = 30
    MAX_SL_CLUSTER_HITS: int = 3
    
    # RomeOPT scoring
    ROME_BASE_SCORE: int = 11  # Perfect score for Rome sequence

# ==================== PURE ROMEOPT INSTITUTIONAL SEQUENCING ====================

class PureRomeAnalyzer:
    """PURE ROMEOPT INSTITUTIONAL SEQUENCING - 6 STEPS ONLY"""
    
    def __init__(self):
        self.sequence_complete = False
        self.current_step = 0
        self.rejection_reason = ""
        
    def generate_signal(self, df: pd.DataFrame, symbol: str, context=None) -> Optional[Dict]:
        """PURE ROMEOPT 6-STEP INSTITUTIONAL SEQUENCE - ONLY CURRENT SIGNALS"""
        if context is None:
            context = {}
            
        self.sequence_complete = False
        self.current_step = 0
        
        if df is None or len(df) < 20:
            return None

        try:
            # 🚨 CRITICAL: Get CURRENT price and timestamp
            current_price = df["close"].iloc[-1]
            current_timestamp = df.index[-1] if hasattr(df.index, 'iloc') else len(df) - 1
            context['current_price'] = current_price
            
            # 🚨 CHECK: Only analyze VERY RECENT patterns (last 1-2 candles)
            # Reject any signal that's based on old data
            max_lookback_candles = 3  # Only check last 3 candles for patterns
            
            # 🔥 STEP 1: Liquidity Sweep Condition - MUST BE RECENT
            sweep_result = self._check_recent_liquidity_sweep(df, max_lookback_candles)
            if not sweep_result["valid"]:
                self.rejection_reason = f"Step 1 failed: {sweep_result['reason']}"
                return None
            self.current_step = 1
            
            # 🔥 STEP 2: Displacement Condition - MUST BE RECENT  
            displacement_result = self._check_recent_displacement(df, sweep_result, max_lookback_candles)
            if not displacement_result["valid"]:
                self.rejection_reason = f"Step 2 failed: {displacement_result['reason']}"
                return None
            self.current_step = 2
            
            # 🔥 STEP 3: Retracement Into Zone - MUST BE CURRENT
            zone_result = self._check_current_retracement(df, displacement_result, context)
            if not zone_result["valid"]:
                self.rejection_reason = f"Step 3 failed: {zone_result['reason']}"
                return None
            self.current_step = 3
            
            # 🔥 STEP 4: Premium/Discount Filter - CURRENT PRICE
            equilibrium_result = self._check_premium_discount(df, zone_result, context)
            if not equilibrium_result["valid"]:
                self.rejection_reason = f"Step 4 failed: {equilibrium_result['reason']}"
                return None
            self.current_step = 4
            
            # 🔥 STEP 5: HTF Bias Alignment
            htf_result = self._check_htf_alignment(df, equilibrium_result, context)
            if not htf_result["valid"]:
                self.rejection_reason = f"Step 5 failed: {htf_result['reason']}"
                return None
            self.current_step = 5
            
            # 🔥 STEP 6: Momentum & Volatility Confirmation - CURRENT
            momentum_result = self._check_current_momentum(df, htf_result, context)
            if not momentum_result["valid"]:
                self.rejection_reason = f"Step 6 failed: {momentum_result['reason']}"
                return None
            self.current_step = 6
            
            # ✅ ALL ROME CONDITIONS MET - GENERATE SIGNAL
            self.sequence_complete = True
            
            # 🚨 FINAL VALIDATION: Ensure this is a CURRENT signal
            signal_age = self._get_signal_age(sweep_result, displacement_result)
            if signal_age > max_lookback_candles:
                logging.info(f"⏰ REJECTED OLD SIGNAL: {symbol} - Signal age: {signal_age} candles")
                return None
                
            signal = self._format_pure_rome_signal(momentum_result, symbol, context)
            
            # 🚨 DEBUG: Log signal timing
            if signal:
                logging.info(f"🎯 CURRENT ROME SIGNAL: {symbol} | Entry: {signal['entry']} | Fresh signal detected")
            
            return signal
            
        except Exception as e:
            logging.error(f"Rome sequencing error for {symbol}: {e}")
            return None

    def _check_recent_liquidity_sweep(self, df: pd.DataFrame, max_lookback: int) -> Dict:
        """STEP 1: Check for RECENT liquidity sweep only"""
        if len(df) < 10:
            return {"valid": False, "reason": "Insufficient data"}
            
        # 🚨 ONLY check recent candles (last N candles)
        recent_candles = df.iloc[-max_lookback:]
        
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
                    "sweep_index": -len(recent_candles) + i,
                    "sweep_recency": i  # How recent this sweep is
                }
            
            # Sweep of equal lows  
            if self._is_equal_low_sweep(current, previous, lookback_candles):
                return {
                    "valid": True, 
                    "type": "equal_low_sweep", 
                    "direction": "bullish",
                    "sweep_index": -len(recent_candles) + i,
                    "sweep_recency": i
                }
            
            # Stop-run wick above/below previous swing
            stop_run = self._is_stop_run_sweep(current, df)
            if stop_run["valid"]:
                stop_run["sweep_index"] = -len(recent_candles) + i
                stop_run["sweep_recency"] = i
                return stop_run
        
        return {"valid": False, "reason": "No RECENT liquidity sweep detected"}

    def _check_recent_displacement(self, df: pd.DataFrame, sweep_result: Dict, max_lookback: int) -> Dict:
        """STEP 2: Check for RECENT displacement after sweep"""
        sweep_recency = sweep_result.get("sweep_recency", 1)
        
        # 🚨 Only look for displacement in very recent candles after sweep
        max_displacement_lookback = min(3, max_lookback - sweep_recency)
        
        if max_displacement_lookback <= 0:
            return {"valid": False, "reason": "Sweep too recent for displacement"}
            
        sweep_idx = sweep_result.get("sweep_index", -5)
        start_idx = max(0, len(df) + sweep_idx + 1)
        post_sweep_candles = df.iloc[start_idx:start_idx + max_displacement_lookback]
        
        if len(post_sweep_candles) == 0:
            return {"valid": False, "reason": "No candles after sweep"}
        
        impulse_candle = None
        for i in range(len(post_sweep_candles)):
            candle = post_sweep_candles.iloc[i]
            body_size = abs(candle["close"] - candle["open"])
            full_range = candle["high"] - candle["low"]
            
            if full_range > 0 and (body_size / full_range) >= 0.6:
                impulse_candle = candle
                break
        
        if impulse_candle is None:
            return {"valid": False, "reason": "No RECENT impulse candle"}
        
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
            "displacement_recency": sweep_recency + i + 1
        }

    def _check_current_retracement(self, df: pd.DataFrame, displacement_result: Dict, context: Dict) -> Dict:
        """STEP 3: Check CURRENT retracement into zone"""
        current_price = df["close"].iloc[-1]
        direction = displacement_result["direction"]
        
        # 🚨 Only look for zones that are CURRENTLY being touched
        fvg_zone = self._find_fvg_zone(df, direction)
        if fvg_zone and self._price_in_zone(current_price, fvg_zone):
            # 🚨 Verify this is a recent zone
            if self._is_recent_zone(df, fvg_zone):
                return {
                    "valid": True, 
                    "zone_type": "fvg", 
                    "zone": fvg_zone,
                    "direction": direction
                }
        
        ob_zone = self._find_order_block(df, direction)
        if ob_zone and self._price_in_zone(current_price, ob_zone):
            # 🚨 Verify this is a recent zone
            if self._is_recent_zone(df, ob_zone):
                return {
                    "valid": True, 
                    "zone_type": "order_block", 
                    "zone": ob_zone,
                    "direction": direction
                }
        
        return {"valid": False, "reason": "No CURRENT mitigation zone touched"}

    def _check_current_momentum(self, df: pd.DataFrame, htf_result: Dict, context: Dict) -> Dict:
        """STEP 6: Check CURRENT momentum only"""
        direction = htf_result["direction"]
        current_price = df["close"].iloc[-1]
        
        # 🚨 STRICT: Only use CURRENT candle for momentum
        current_candle = df.iloc[-1]
        
        if direction == "bullish":
            if not (current_candle["close"] > current_candle["open"]):
                return {"valid": False, "reason": "No CURRENT bullish momentum"}
        else:
            if not (current_candle["close"] < current_candle["open"]):
                return {"valid": False, "reason": "No CURRENT bearish momentum"}
        
        # Additional volatility check
        atr_val = self._calculate_atr(df.tail(14))
        if atr_val and atr_val < current_price * 0.001:
            return {"valid": False, "reason": "Volatility too low"}
        
        return {
            "valid": True,
            "momentum_confirmed": True,
            "volatility_acceptable": True,
            "direction": direction
        }

    def _is_recent_zone(self, df: pd.DataFrame, zone: Dict) -> bool:
        """Check if a zone was formed recently"""
        # 🚨 Only accept zones formed in the last 10 candles
        max_zone_age = 10
        zone_low = zone["low"]
        zone_high = zone["high"]
        
        # Check recent candles to see if this zone was recently formed
        recent_data = df.tail(max_zone_age * 2)  # Look at more data for context
        
        for i in range(len(recent_data) - 1):
            candle = recent_data.iloc[i]
            next_candle = recent_data.iloc[i + 1]
            
            # Check for FVG formation
            if (next_candle["low"] > candle["high"] and 
                zone_low == candle["high"] and zone_high == next_candle["low"]):
                return True
                
            # Check for OB formation  
            if (abs(candle["close"] - candle["open"]) / (candle["high"] - candle["low"]) >= 0.6):
                if zone_low == candle["low"] and zone_high == candle["open"]:
                    return True
                if zone_low == candle["close"] and zone_high == candle["high"]:
                    return True
                    
        return False

    def _get_signal_age(self, sweep_result: Dict, displacement_result: Dict) -> int:
        """Calculate how old the signal pattern is in candles"""
        sweep_recency = sweep_result.get("sweep_recency", 10)  # Default to old if not specified
        displacement_recency = displacement_result.get("displacement_recency", 10)
        
        # Total age is the recency of the sweep + displacement
        return sweep_recency + displacement_recency

    def _format_pure_rome_signal(self, final_result: Dict, symbol: str, context: Dict) -> Dict:
        """Format pure RomeOPT signal"""
        direction = final_result["direction"]
        side = "BUY" if direction == "bullish" else "SELL"
        
        current_price = context.get('current_price', 0)
        tf = context.get('tf', '15m')
        
        # 🚨 FINAL PRICE VALIDATION
        if current_price == 0:
            logging.error(f"🚨 ZERO PRICE in signal formatting for {symbol}")
            return None
            
        sl, tp1, tp2, tp3 = self._calculate_institutional_tpsl(current_price, side, tf)
        
        return {
            "symbol": symbol,
            "side": side,
            "entry": current_price,
            "sl": sl,
            "tp1": tp1,
            "tp2": tp2,
            "tp3": tp3,
            "score": 11,
            "reason": "PURE ROMEOPT INSTITUTIONAL SEQUENCE",
            "reason_list": [
                "✅ Recent Liquidity Sweep", 
                "✅ Recent Displacement", 
                "✅ Current Zone Retracement",
                "✅ Premium/Discount", 
                "✅ HTF Alignment", 
                "✅ Current Momentum"
            ],
            "timeframe": tf,
            "rome_sequence": True,
            "sequence_steps_passed": 6
        }

    # ... keep all your other existing helper methods the same ...

    # ==================== KEEP ALL YOUR EXISTING METHODS EXACTLY AS THEY ARE ====================
    
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
        for i in range(len(post_sweep_candles)):
            candle = post_sweep_candles.iloc[i]
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
        
        # For different timeframes, use appropriate HTF data
        current_tf = context.get('tf', '15m')
        if current_tf in ['1m', '3m', '5m']:
            htf_data = context.get('df_15m')  # Use 15m as HTF for low timeframes
        elif current_tf == '15m':
            htf_data = context.get('df_1h')  # Use 1h as HTF for 15m
        elif current_tf == '30m':
            htf_data = context.get('df_4h') or context.get('df_1h')  # Use 4h/1h for 30m
        else:
            htf_data = context.get('df_4h')  # Use 4h for 1h+
        
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
        
        # For very low timeframes, be more lenient with momentum
        current_tf = context.get('tf', '15m')
        if current_tf in ['1m', '3m']:
            # Less strict momentum for 1m/3m
            if len(df) >= 2:
                current_candle = df.iloc[-1]
                if direction == "bullish":
                    if not (current_candle["close"] > current_candle["open"]):
                        return {"valid": False, "reason": "No bullish momentum confirmation"}
                else:
                    if not (current_candle["close"] < current_candle["open"]):
                        return {"valid": False, "reason": "No bearish momentum confirmation"}
        else:
            # Standard momentum check for higher timeframes
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

    def _calculate_institutional_tpsl(self, entry: float, side: str, timeframe: str) -> tuple:
        """Institutional-grade TP/SL calculation with timeframe adjustments"""
        # Adjust risk based on timeframe
        tf_multiplier = {
            '1m': 0.3,   # Tighter stops for low timeframes
            '3m': 0.5,
            '5m': 0.7,
            '15m': 1.0,  # Standard for 15m
            '30m': 1.2,
            '1h': 1.5    # Wider stops for higher timeframes
        }
        
        multiplier = tf_multiplier.get(timeframe, 1.0)
        
        if side == "BUY":
            sl_pct = 0.002 * multiplier
            tp1_pct = 0.004 * multiplier
            tp2_pct = 0.008 * multiplier
            tp3_pct = 0.012 * multiplier
            
            sl = entry * (1 - sl_pct)
            tp1 = entry * (1 + tp1_pct)
            tp2 = entry * (1 + tp2_pct)
            tp3 = entry * (1 + tp3_pct)
        else:
            sl_pct = 0.002 * multiplier
            tp1_pct = 0.004 * multiplier
            tp2_pct = 0.008 * multiplier
            tp3_pct = 0.012 * multiplier
            
            sl = entry * (1 + sl_pct)
            tp1 = entry * (1 - tp1_pct)
            tp2 = entry * (1 - tp2_pct)
            tp3 = entry * (1 - tp3_pct)
            
        return sl, tp1, tp2, tp3

    def _calculate_atr(self, df: pd.DataFrame, period: int = 14) -> float:
        if len(df) < period:
            return 0.0
            
        high, low, close = df["high"], df["low"], df["close"]
        tr1 = high - low
        tr2 = (high - close.shift(1)).abs()
        tr3 = (low - close.shift(1)).abs()
        true_range = pd.DataFrame({'tr1': tr1, 'tr2': tr2, 'tr3': tr3}).max(axis=1)
        return true_range.rolling(period).mean().iloc[-1]

    def _find_swing_highs(self, df: pd.DataFrame, lookback: int = 3) -> List[float]:
        if len(df) < lookback * 2 + 1:
            return []
            
        highs = []
        for i in range(lookback, len(df) - lookback):
            if (df["high"].iloc[i] == df["high"].iloc[i-lookback:i+lookback+1].max()):
                highs.append(df["high"].iloc[i])
        return highs

    def _find_swing_lows(self, df: pd.DataFrame, lookback: int = 3) -> List[float]:
        if len(df) < lookback * 2 + 1:
            return []
            
        lows = []
        for i in range(lookback, len(df) - lookback):
            if (df["low"].iloc[i] == df["low"].iloc[i-lookback:i+lookback+1].min()):
                lows.append(df["low"].iloc[i])
        return lows

    def _find_fvg_zone(self, df: pd.DataFrame, direction: str) -> Optional[Dict]:
        if len(df) < 3:
            return None
            
        for i in range(len(df) - 3, max(0, len(df) - 10), -1):
            if i + 2 >= len(df):
                continue
                
            c1, c2 = df.iloc[i], df.iloc[i+1]
            
            if direction == "bullish" and c2["low"] > c1["high"]:
                return {"low": c1["high"], "high": c2["low"], "type": "bullish_fvg"}
            elif direction == "bearish" and c2["high"] < c1["low"]:
                return {"low": c2["high"], "high": c1["low"], "type": "bearish_fvg"}
                
        return None

    def _find_order_block(self, df: pd.DataFrame, direction: str) -> Optional[Dict]:
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

    def _price_in_zone(self, price: float, zone: Dict) -> bool:
        return zone["low"] <= price <= zone["high"]

    def _detect_htf_trend(self, htf_df: pd.DataFrame) -> str:
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

# ==================== ENHANCED DATA MODELS ====================

@dataclass
class RomeSignal:
    """Pure RomeOPT signal tracking"""
    symbol: str
    side: SignalSide
    entry_price: float
    stop_loss: float
    take_profit_1: float
    take_profit_2: float
    take_profit_3: float
    timestamp: datetime.datetime
    timeframe: str
    score: int
    sequence_steps_passed: int
    signal_id: str
    status: str = "active"

# ==================== FIXED TRADE MONITORING SYSTEM ====================

class RomeTradeMonitor:
    """FIXED Advanced monitoring for RomeOPT signals"""
    
    def __init__(self, scanner):
        self.scanner = scanner
        self.open_signals = {}
        self.closed_trades = []
        self.all_signals = []
        self.last_summary_time = time.time()
        self.recent_sl = defaultdict(lambda: deque())
        
    async def add_signal(self, signal: RomeSignal):
        """Add Rome signal to monitoring"""
        self.open_signals[signal.signal_id] = signal
        self.all_signals.append({
            'signal': signal,
            'status': 'OPEN',
            'added_time': datetime.datetime.utcnow()
        })
        # 🚨 DEBUG: Log the entry price when signal is added
        logging.info(f"🏛️ Rome Monitoring ADDED: {signal.symbol} {signal.side.value} | "
                   f"Entry: {signal.entry_price:.6f} | TF: {signal.timeframe} | Score: {signal.score}")
        
    def record_sl_hit(self, symbol: str, lookback_minutes=30):
        """Track SL hits for deprioritization"""
        now = time.time()
        dq = self.recent_sl[symbol]
        dq.append(now)
        cutoff = now - lookback_minutes * 60
        while dq and dq[0] < cutoff: 
            dq.popleft()
        
    def deprioritized(self, symbol: str, threshold=3, lookback=30):
        """Check if symbol should be deprioritized"""
        dq = self.recent_sl[symbol]
        now = time.time()
        cutoff = now - lookback * 60
        while dq and dq[0] < cutoff: 
            dq.popleft()
        return len(dq) >= threshold

    async def monitor_open_signals(self):
        """FIXED: Monitor Rome signals and update their status"""
        if not self.open_signals: 
            return
        
        signals_to_remove = []
        
        for signal_id, signal in self.open_signals.items():
            try:
                ticker = await self.scanner.fetch_ticker(signal.symbol)
                if not ticker:
                    continue
                    
                current_price = ticker['last']
                
                # 🚨 DEBUG: Log current price vs entry price
                logging.info(f"🔍 MONITORING: {signal.symbol} | Entry: {signal.entry_price:.6f} | Current: {current_price:.6f} | Diff: {((current_price - signal.entry_price) / signal.entry_price * 100):+.2f}%")
                
                status = await self.check_signal_status(signal, current_price)
                
                # 🚨 CRITICAL FIX: Update the signal status and process if closed
                if status != "OPEN" and signal.status == "active":
                    signal.status = status.lower()  # Update the signal object status
                    await self._process_closed_signal(signal, status, current_price)
                    signals_to_remove.append(signal_id)
                    if "SL" in status:
                        self.record_sl_hit(signal.symbol)
                    
            except Exception as e:
                logging.error(f"Error monitoring {signal.symbol}: {e}")
        
        # Remove closed signals from open_signals
        for signal_id in signals_to_remove:
            if signal_id in self.open_signals:
                del self.open_signals[signal_id]

    async def check_signal_status(self, signal: RomeSignal, current_price: float):
        """FIXED: Check TP/SL hits for Rome signals with detailed logging"""
        try:
            if signal.side == SignalSide.BUY:
                if current_price >= signal.take_profit_3:
                    logging.info(f"🎯 TP3 HIT: {signal.symbol} | Entry: {signal.entry_price:.6f} | Price: {current_price:.6f} >= TP3: {signal.take_profit_3:.6f}")
                    return "TP3_HIT"
                elif current_price >= signal.take_profit_2:
                    logging.info(f"🎯 TP2 HIT: {signal.symbol} | Entry: {signal.entry_price:.6f} | Price: {current_price:.6f} >= TP2: {signal.take_profit_2:.6f}")
                    return "TP2_HIT"
                elif current_price >= signal.take_profit_1:
                    logging.info(f"🎯 TP1 HIT: {signal.symbol} | Entry: {signal.entry_price:.6f} | Price: {current_price:.6f} >= TP1: {signal.take_profit_1:.6f}")
                    return "TP1_HIT"
                elif current_price <= signal.stop_loss:
                    logging.info(f"🛑 SL HIT: {signal.symbol} | Entry: {signal.entry_price:.6f} | Price: {current_price:.6f} <= SL: {signal.stop_loss:.6f}")
                    return "SL_HIT"
            else:  # SELL
                if current_price <= signal.take_profit_3:
                    logging.info(f"🎯 TP3 HIT: {signal.symbol} | Entry: {signal.entry_price:.6f} | Price: {current_price:.6f} <= TP3: {signal.take_profit_3:.6f}")
                    return "TP3_HIT"
                elif current_price <= signal.take_profit_2:
                    logging.info(f"🎯 TP2 HIT: {signal.symbol} | Entry: {signal.entry_price:.6f} | Price: {current_price:.6f} <= TP2: {signal.take_profit_2:.6f}")
                    return "TP2_HIT"
                elif current_price <= signal.take_profit_1:
                    logging.info(f"🎯 TP1 HIT: {signal.symbol} | Entry: {signal.entry_price:.6f} | Price: {current_price:.6f} <= TP1: {signal.take_profit_1:.6f}")
                    return "TP1_HIT"
                elif current_price >= signal.stop_loss:
                    logging.info(f"🛑 SL HIT: {signal.symbol} | Entry: {signal.entry_price:.6f} | Price: {current_price:.6f} >= SL: {signal.stop_loss:.6f}")
                    return "SL_HIT"
            
            return "OPEN"
        except Exception as e:
            logging.error(f"Signal status check error: {e}")
            return "OPEN"

    async def _process_closed_signal(self, signal: RomeSignal, status: str, close_price: float):
        """FIXED: Process closed Rome signal with enhanced tracking"""
        try:
            # Calculate P&L
            if signal.side == SignalSide.BUY:
                pnl_pct = (close_price - signal.entry_price) / signal.entry_price * 100
            else:
                pnl_pct = (signal.entry_price - close_price) / signal.entry_price * 100
            
            # Calculate duration
            duration = (datetime.datetime.utcnow() - signal.timestamp).total_seconds() / 60  # minutes
            
            # Create comprehensive trade record
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
                'duration_minutes': duration,
                'timeframe': signal.timeframe,
                'score': signal.score,
                'sequence_steps': signal.sequence_steps_passed,
                'stop_loss': signal.stop_loss,
                'take_profit_1': signal.take_profit_1,
                'take_profit_2': signal.take_profit_2,
                'take_profit_3': signal.take_profit_3
            }
            
            self.closed_trades.append(trade_record)
            
            # Update all_signals record with detailed info
            for sig_data in self.all_signals:
                if sig_data['signal'].signal_id == signal.signal_id:
                    sig_data['status'] = status
                    sig_data['close_price'] = close_price
                    sig_data['pnl_pct'] = pnl_pct
                    sig_data['exit_time'] = datetime.datetime.utcnow()
                    sig_data['duration_minutes'] = duration
                    break
            
            await self._send_trade_update(signal, status, close_price, pnl_pct, duration)
            logging.info(f"🎯 Rome Trade CLOSED: {signal.symbol} {signal.timeframe} {status} | "
                        f"Entry: {signal.entry_price:.6f} | Exit: {close_price:.6f} | "
                        f"P&L: {pnl_pct:+.2f}% | Score: {signal.score} | Duration: {duration:.1f}m")
            
        except Exception as e:
            logging.error(f"Process closed signal error: {e}")

    async def _send_trade_update(self, signal: RomeSignal, status: str, close_price: float, pnl_pct: float, duration: float):
        """Send enhanced Rome trade update with all details"""
        try:
            emoji = "🟢" if "TP" in status else "🔴"
            pnl_emoji = "📈" if pnl_pct > 0 else "📉"
            
            # 🚨 ADDED: Entry vs Exit comparison
            price_diff = close_price - signal.entry_price
            price_diff_pct = (price_diff / signal.entry_price) * 100
            
            message = f"""
{emoji} **🏛️ ROMEOPT TRADE CLOSED** {emoji}

Symbol: {signal.symbol}
Timeframe: {signal.timeframe}
Side: {signal.side.value}
Status: {status}

Entry: {signal.entry_price:.6f}
Exit: {close_price:.6f}
Price Change: {price_diff:+.6f} ({price_diff_pct:+.2f}%)
Duration: {duration:.1f} minutes

{pnl_emoji} P&L: {pnl_pct:+.2f}%

Risk Levels:
SL: {signal.stop_loss:.6f}
TP1: {signal.take_profit_1:.6f}
TP2: {signal.take_profit_2:.6f}  
TP3: {signal.take_profit_3:.6f}

Rome Score: {signal.score}/11
Sequence Steps: {signal.sequence_steps_passed}/6
"""
            await send_telegram_message(message)
        except Exception as e:
            logging.error(f"Send trade update error: {e}")

    async def log_monitoring_status(self):
        """Log current monitoring status for debugging"""
        try:
            if self.open_signals:
                logging.info(f"📊 Currently monitoring {len(self.open_signals)} open signals:")
                for signal_id, signal in list(self.open_signals.items())[:5]:  # Show first 5
                    # Get current price for comparison
                    try:
                        ticker = await self.scanner.fetch_ticker(signal.symbol)
                        current_price = ticker['last'] if ticker else 0
                        price_diff_pct = ((current_price - signal.entry_price) / signal.entry_price * 100) if current_price else 0
                        
                        logging.info(f"   - {signal.symbol} {signal.timeframe} {signal.side.value} | "
                                   f"Entry: {signal.entry_price:.6f} | Current: {current_price:.6f} | "
                                   f"Diff: {price_diff_pct:+.2f}% | Status: {signal.status}")
                    except:
                        logging.info(f"   - {signal.symbol} {signal.timeframe} {signal.side.value} | "
                                   f"Entry: {signal.entry_price:.6f} | Status: {signal.status}")
            else:
                logging.info("📊 No open signals currently being monitored")
                
        except Exception as e:
            logging.error(f"Monitoring status log error: {e}")

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
            
            # Group by timeframe
            tf_stats = {}
            for signal in recent_signals:
                tf = signal['signal'].timeframe
                if tf not in tf_stats:
                    tf_stats[tf] = {'total': 0, 'winning': 0}
                tf_stats[tf]['total'] += 1
                if signal.get('pnl_pct', 0) > 0:
                    tf_stats[tf]['winning'] += 1

            total_signals = len(recent_signals)
            win_rate = len(winning_trades) / len(closed_signals) * 100 if closed_signals else 0
            avg_score = sum(s['signal'].score for s in recent_signals) / total_signals if total_signals else 0

            message = f"""
📊 **🏛️ ROMEOPT 2-HOUR PERFORMANCE** 📊

⏰ Period: Last 2 hours
📈 Total Signals: {total_signals}
🟢 Open Signals: {len(open_signals)}
🔒 Closed Signals: {len(closed_signals)}
🎯 Win Rate: {win_rate:.1f}%
⭐ Avg Rome Score: {avg_score:.1f}/11

📋 **TIMEFRAME STATS:**
"""
            
            for tf, stats in tf_stats.items():
                tf_win_rate = (stats['winning'] / stats['total'] * 100) if stats['total'] > 0 else 0
                message += f"• {tf}: {stats['total']} signals, {tf_win_rate:.1f}% win rate\n"

            message += "\n📋 **RECENT ROME SIGNALS:**"
            
            for i, sig_data in enumerate(recent_signals[-5:], 1):
                signal = sig_data['signal']
                status = sig_data['status']
                pnl = sig_data.get('pnl_pct', 0)
                
                status_emoji = "🟢" if "TP" in status else "🔴" if status == "SL_HIT" else "🟡"
                pnl_str = f"{pnl:+.2f}%" if status != "OPEN" else "OPEN"
                
                message += f"\n{i}. {status_emoji} {signal.symbol} {signal.timeframe} {signal.side.value} | Entry: {signal.entry_price:.6f} | Score: {signal.score} | {pnl_str}"
            
            await send_telegram_message(message)
            logging.info("📊 RomeOPT 2-hour performance summary sent")
            return True
        except Exception as e:
            logging.error(f"Performance summary error: {e}")
            return False

    def get_performance_stats(self):
        """Get Rome performance statistics"""
        try:
            if not self.closed_trades:
                return {"total_trades": 0, "win_rate": 0, "avg_pnl": 0, "total_pnl": 0}
            
            winning_trades = [t for t in self.closed_trades if t['pnl_pct'] > 0]
            total_pnl = sum(t['pnl_pct'] for t in self.closed_trades)
            
            # Stats by timeframe
            tf_stats = {}
            for trade in self.closed_trades:
                tf = trade['timeframe']
                if tf not in tf_stats:
                    tf_stats[tf] = {'total': 0, 'winning': 0, 'total_pnl': 0}
                tf_stats[tf]['total'] += 1
                tf_stats[tf]['total_pnl'] += trade['pnl_pct']
                if trade['pnl_pct'] > 0:
                    tf_stats[tf]['winning'] += 1
            
            stats = {
                'total_trades': len(self.closed_trades),
                'winning_trades': len(winning_trades),
                'win_rate': len(winning_trades) / len(self.closed_trades) * 100 if self.closed_trades else 0,
                'avg_pnl': total_pnl / len(self.closed_trades) if self.closed_trades else 0,
                'total_pnl': total_pnl,
                'avg_rome_score': sum(t['score'] for t in self.closed_trades) / len(self.closed_trades) if self.closed_trades else 0,
                'timeframe_stats': tf_stats
            }
            return stats
        except Exception as e:
            logging.error(f"Performance stats error: {e}")
            return {"total_trades": 0, "win_rate": 0, "avg_pnl": 0, "total_pnl": 0}
# ==================== FIXED PURE ROMEOPT SCANNER ====================

class PureRomeScanner:
    """FIXED PURE ROMEOPT SCANNER - 6-STEP INSTITUTIONAL SEQUENCING ONLY"""
    
    def __init__(self, config: PureRomeConfig):
        self.config = config
        self.trade_monitor = RomeTradeMonitor(self)
        self.bingx_api = BingXAPI()
        self.signal_cooldown = {}
        self.rome_analyzer = PureRomeAnalyzer()
        
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
        logging.info("🏛️ PURE ROMEOPT SCANNER INITIALIZED")
        logging.info("✅ 6-Step Institutional Sequencing Active")
        logging.info("✅ All Timeframes: 1m, 3m, 5m, 15m, 30m, 1h")
        logging.info("✅ No Additional Filters - Pure Rome Logic Only")
        logging.info("✅ BingX API Integration Active")
        logging.info("✅ FIXED TP/SL Tracking System")
        logging.info("🚨 FIXED PRICE CONSISTENCY - NO MORE LATE SIGNALS")

    async def initialize_exchange(self):
        """Initialize with BingX API"""
        try:
            connectivity_ok = await self.bingx_api.test_connectivity()
            
            if connectivity_ok:
                logging.info("✅ BingX API connectivity test PASSED")
                return True
            else:
                logging.error("❌ BingX API connectivity test FAILED")
                return False
                
        except Exception as e:
            logging.error(f"❌ BingX API initialization failed: {e}")
            return False

    async def fetch_ohlcv_data(self, symbol: str, timeframe: str, limit: int = 200) -> Optional[pd.DataFrame]:
        """Fetch OHLCV data using BingX API"""
        try:
            ohlcv = await self.bingx_api.fetch_ohlcv(symbol, timeframe, limit)
            if not ohlcv or len(ohlcv) < 20: 
                return None
                
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            return df
            
        except Exception as e:
            logging.debug(f"Could not fetch {symbol} {timeframe} from BingX: {e}")
            return None

    async def fetch_ticker(self, symbol: str) -> Optional[Dict]:
        """Fetch ticker using BingX API"""
        try:
            return await self.bingx_api.fetch_ticker(symbol)
        except Exception as e:
            logging.error(f"BingX ticker fetch error for {symbol}: {e}")
            return None

    async def get_btc_context(self) -> Dict[str, Any]:
        """Get BTC context for HTF alignment"""
        try:
            btc_15m = await self.fetch_ohlcv_data('BTC-USDT', '15m', 100)
            btc_1h = await self.fetch_ohlcv_data('BTC-USDT', '1h', 100)
            btc_4h = await self.fetch_ohlcv_data('BTC-USDT', '4h', 100)
            return {
                'df_15m': btc_15m,
                'df_1h': btc_1h,
                'df_4h': btc_4h
            }
        except Exception as e:
            logging.error(f"Error getting BTC context from BingX: {e}")
            return {}

    async def scan_symbol(self, symbol: str) -> List[RomeSignal]:
        """PURE ROMEOPT SCANNING - ALL TIMEFRAMES"""
        signals = []
        
        try:
            context = await self.get_btc_context()
            
            # ALL TIMEFRAMES INCLUDED
            timeframes = ["1m", "3m", "5m", "15m", "30m", "1h"]
            
            for tf in timeframes:
                cooldown_key = f"{symbol}_{tf}"
                if cooldown_key in self.signal_cooldown:
                    if time.time() - self.signal_cooldown[cooldown_key] < self.config.COOLDOWN_MINUTES * 60:
                        continue
                
                if self.trade_monitor.deprioritized(symbol):
                    continue
                
                bingx_symbol = symbol.replace('/', '-')
                df = await self.fetch_ohlcv_data(bingx_symbol, tf)
                if df is None or len(df) < 20:
                    continue
                
                scan_context = context.copy()
                scan_context['tf'] = tf
                scan_context['current_price'] = df['close'].iloc[-1]  # Fresh price
                
                # Get appropriate HTF data for each timeframe
                if tf in ['1m', '3m', '5m']:
                    df_15m = await self.fetch_ohlcv_data(bingx_symbol, '15m', 100)
                    scan_context['df_15m'] = df_15m
                elif tf == '15m':
                    df_1h = await self.fetch_ohlcv_data(bingx_symbol, '1h', 100)
                    scan_context['df_1h'] = df_1h
                elif tf == '30m':
                    df_4h = await self.fetch_ohlcv_data(bingx_symbol, '4h', 100)
                    scan_context['df_1h'] = df_4h  # Use as proxy
                elif tf == '1h':
                    df_4h = await self.fetch_ohlcv_data(bingx_symbol, '4h', 100)
                    scan_context['df_4h'] = df_4h
                
                rome_signal = self.rome_analyzer.generate_signal(df, symbol, scan_context)
                if not rome_signal: 
                    continue
                
                pure_signal = await self._create_rome_signal(rome_signal)
                if pure_signal:
                    if await self._validate_signal(pure_signal):
                        signals.append(pure_signal)
                        self.signal_cooldown[cooldown_key] = time.time()
                        await self.trade_monitor.add_signal(pure_signal)
                        
                        await self._send_rome_notification(pure_signal, rome_signal)
                        
                        logging.info(f"🏛️ PURE ROME SIGNAL: {symbol} {pure_signal.timeframe} {pure_signal.side.value} | Score: {pure_signal.score}")
        
        except Exception as e:
            logging.error(f"Error scanning {symbol}: {e}")
            
        return signals

    async def _create_rome_signal(self, rome_signal: Dict) -> Optional[RomeSignal]:
        """Create pure Rome signal object"""
        try:
            signal = RomeSignal(
                symbol=rome_signal['symbol'],
                side=SignalSide.BUY if rome_signal['side'] == 'BUY' else SignalSide.SELL,
                entry_price=rome_signal['entry'],
                stop_loss=rome_signal['sl'],
                take_profit_1=rome_signal['tp1'],
                take_profit_2=rome_signal['tp2'],
                take_profit_3=rome_signal['tp3'],
                timestamp=datetime.datetime.utcnow(),
                timeframe=rome_signal['timeframe'],
                score=rome_signal['score'],
                sequence_steps_passed=rome_signal['sequence_steps_passed'],
                signal_id=f"ROME_{rome_signal['symbol']}_{rome_signal['timeframe']}_{int(time.time())}"
            )
            
            return signal
        except Exception as e:
            logging.error(f"Create Rome signal error: {e}")
            return None

    async def _send_rome_notification(self, rome_signal: RomeSignal, signal_data: Dict):
        """Send pure Rome signal notification"""
        try:
            message = f"""
🏛️ **PURE ROMEOPT INSTITUTIONAL SIGNAL** 🏛️

Symbol: {rome_signal.symbol}
Timeframe: {rome_signal.timeframe}
Side: {rome_signal.side.value}
Entry: {rome_signal.entry_price:.6f}

Risk Management:
SL: {rome_signal.stop_loss:.6f}
TP1: {rome_signal.take_profit_1:.6f}
TP2: {rome_signal.take_profit_2:.6f}
TP3: {rome_signal.take_profit_3:.6f}

🏆 ROME SCORE: {rome_signal.score}/11
🎯 Sequence Steps: {rome_signal.sequence_steps_passed}/6

Institutional Sequence:
{' • '.join(signal_data['reason_list'])}
"""
            await send_telegram_message(message)
        except Exception as e:
            logging.error(f"Send Rome notification error: {e}")

    async def _validate_signal(self, signal: RomeSignal) -> bool:
        """FIXED: Final validation - only block if same symbol/timeframe has ACTIVE signal"""
        try:
            for signal_id, open_signal in self.trade_monitor.open_signals.items():
                if (open_signal.symbol == signal.symbol and 
                    open_signal.timeframe == signal.timeframe):
                    # Only block if there's already an ACTIVE signal for this symbol/timeframe
                    logging.info(f"⏸️ Already monitoring {signal.symbol} on {signal.timeframe}")
                    return False
                    
            return True
        except Exception as e:
            logging.error(f"Signal validation error: {e}")
            return False

    async def get_top_symbols(self) -> List[str]:
        """Get top symbols with volume filter"""
        try:
            tickers = await self.bingx_api.fetch_tickers()
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
            
            logging.info(f"📊 Selected {len(top_symbols)} elite symbols from BingX")
            return top_symbols
            
        except Exception as e:
            logging.error(f"Error getting top symbols from BingX: {e}")
            default_symbols = ['BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'ADA/USDT', 'DOT/USDT']
            logging.info(f"Using default symbols: {default_symbols}")
            return default_symbols

    async def run_scan_cycle(self):
        """Pure RomeOPT scanning cycle"""
        try:
            logging.info("🔍 Starting PURE ROMEOPT scan cycle...")
            
            symbols = await self.get_top_symbols()
            if not symbols:
                logging.warning("No symbols to scan from BingX")
                return
                
            all_signals = []
            
            for symbol in symbols:
                try:
                    signals = await self.scan_symbol(symbol)
                    all_signals.extend(signals)
                    await asyncio.sleep(0.1)
                except Exception as e:
                    logging.error(f"Error scanning {symbol} with BingX: {e}")
                    continue
            
            if all_signals:
                # Group signals by timeframe for logging
                tf_counts = {}
                for signal in all_signals:
                    tf = signal.timeframe
                    tf_counts[tf] = tf_counts.get(tf, 0) + 1
                
                tf_info = ", ".join([f"{tf}: {count}" for tf, count in tf_counts.items()])
                logging.info(f"📈 PURE ROMEOPT scan complete: {len(all_signals)} signals across {tf_info}")
            else:
                logging.info("📈 PURE ROMEOPT scan complete: No institutional sequences detected")
                
        except Exception as e:
            logging.error(f"ROMEOPT scan cycle error with BingX: {e}")

    async def start_continuous_scanning(self):
        """FIXED: Pure RomeOPT continuous scanning with enhanced monitoring"""
        logging.info("🔄 Starting PURE ROMEOPT continuous scanning...")
        
        startup_msg = (
            "🏛️ **PURE ROMEOPT INSTITUTIONAL SCANNER STARTED** 🏛️\n"
            "✅ BingX API integration active\n"
            "✅ Strict 6-step RomeOPT sequencing only\n"
            "✅ ALL TIMEFRAMES: 1m, 3m, 5m, 15m, 30m, 1h\n"
            "✅ No additional filters - Pure institutional logic\n"
            "✅ FIXED TP/SL Tracking System\n"
            "🚨 FIXED PRICE CONSISTENCY - NO MORE LATE SIGNALS\n"
            "✅ Real-time signal monitoring active\n"
            "🎯 Target: 100% INSTITUTIONAL-GRADE SIGNALS"
        )
        await send_telegram_message(startup_msg)
        
        try:
            while True:
                start_time = time.time()
                
                await self.run_scan_cycle()
                await self.trade_monitor.monitor_open_signals()
                await self.trade_monitor.log_monitoring_status()  # ✅ ADDED MONITORING STATUS
                await self.trade_monitor.send_performance_summary()
                
                elapsed = time.time() - start_time
                sleep_time = max(1, self.config.SCAN_INTERVAL - elapsed)
                await asyncio.sleep(sleep_time)
                
        except Exception as e:
            logging.error(f"PURE ROMEOPT scanning error: {e}")
            await asyncio.sleep(60)

    async def cleanup(self):
        """Cleanup resources"""
        try:
            logging.info("🧹 PURE ROMEOPT scanner cleanup completed")
        except Exception as e:
            logging.error(f"Cleanup error: {e}")

# ==================== TELEGRAM NOTIFICATIONS ====================

async def send_telegram_message(message: str):
    """Telegram notification function"""
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

scanner: Optional[PureRomeScanner] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage scanner lifecycle"""
    global scanner
    config = PureRomeConfig()
    scanner = PureRomeScanner(config)
    success = await scanner.initialize_exchange()
    
    if success:
        background_tasks = BackgroundTasks()
        background_tasks.add_task(scanner.start_continuous_scanning)
    
    yield
    
    if scanner:
        await scanner.cleanup()

app = FastAPI(title="Pure RomeOPT Institutional Scanner", version="1.0.0", lifespan=lifespan)

class SignalResponse(BaseModel):
    symbol: str
    side: str
    entry_price: float
    score: int
    timeframe: str
    sequence_steps: int
    timestamp: datetime.datetime
    status: str

class PerformanceStats(BaseModel):
    total_trades: int
    win_rate: float
    avg_pnl: float
    open_signals: int
    avg_rome_score: float

@app.get("/")
async def root():
    return {"status": "PURE ROMEOPT INSTITUTIONAL SCANNER - RUNNING"}

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
            score=signal.score,
            timeframe=signal.timeframe,
            sequence_steps=signal.sequence_steps_passed,
            timestamp=signal.timestamp,
            status=signal.status
        ))
    return signals

@app.get("/performance", response_model=PerformanceStats)
async def get_performance():
    if not scanner:
        return PerformanceStats(total_trades=0, win_rate=0, avg_pnl=0, open_signals=0, avg_rome_score=0)
    stats = scanner.trade_monitor.get_performance_stats()
    return PerformanceStats(
        total_trades=stats['total_trades'],
        win_rate=stats['win_rate'],
        avg_pnl=stats['avg_pnl'],
        open_signals=len(scanner.trade_monitor.open_signals),
        avg_rome_score=stats.get('avg_rome_score', 0)
    )

@app.post("/scan-now")
async def trigger_manual_scan():
    """Trigger manual RomeOPT scan cycle"""
    if not scanner:
        raise HTTPException(status_code=500, detail="Scanner not initialized")
    
    asyncio.create_task(scanner.run_scan_cycle())
    return {"status": "PURE ROMEOPT scan triggered"}

# ==================== MAIN EXECUTION ====================

async def main():
    """Pure RomeOPT main execution"""
    try:
        config = PureRomeConfig()
        scanner = PureRomeScanner(config)
        success = await scanner.initialize_exchange()
        
        if not success:
            logging.error("❌ Failed to initialize BingX API. Trying to continue with limited functionality...")
        
        await scanner.start_continuous_scanning()
        
    except KeyboardInterrupt:
        logging.info("🛑 PURE ROMEOPT scanner stopped by user")
    except Exception as e:
        logging.error(f"❌ PURE ROMEOPT scanner error: {e}")
    finally:
        if 'scanner' in locals():
            await scanner.cleanup()

if __name__ == "__main__":
    asyncio.run(main())