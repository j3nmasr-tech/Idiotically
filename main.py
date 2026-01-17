#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CONFLUENCE SCANNER v6.0 - BTC-CENTRIC STRATEGY (MODIFIED)
BITCOIN AS PRIMARY STRUCTURE - Alts must follow BTC direction
When BTC moves, everything moves with it - Trade accordingly
When BTC is neutral, both LONG and SHORT allowed
OKX EXCHANGE INTEGRATION - No geographical restrictions
"""

import os
import time
import asyncio
import logging
import hashlib
import traceback
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Set
from dataclasses import dataclass
import json
from collections import deque

# ================ CONFLUENCE CONFIG ================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DB_PATH = "/app/data/confluence_scanner.db"

# Scanning settings
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 10))
TOP_N_VOLUME = int(os.getenv("TOP_N_VOLUME", 100))
MIN_VOLUME_USD = 100000

# Confluence parameters - TARGET: 3-5% MOVES
TARGET_PROFIT_RANGE = (2.0, 6.0)
MAX_STOP_LOSS = 1.5
MIN_RISK_REWARD = 2.5
MIN_CONFLUENCE_SCORE = 1.0

# BTC-CENTRIC PARAMETERS
BTC_MIN_TREND_STRENGTH = 4.0  # Minimum BTC trend strength to trade alts
BTC_ANALYSIS_INTERVAL = 300   # Re-analyze BTC every 5 minutes

# Timeframes for multi-layer analysis
TIMEFRAMES = {
    "DAILY": "1d",
    "4H": "4h",
    "1H": "1h",
    "15M": "15m",
    "5M": "5m",
}

# Confluence scoring weights
CONFLUENCE_WEIGHTS = {
    "market_structure": 0.25,
    "order_flow": 0.30,
    "momentum": 0.25,
    "liquidity": 0.20,
}

# RSI settings
RSI_PERIOD = 14
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30

# EMA alignment periods
EMA_PERIODS = {
    "fast": 9,
    "medium": 21,
    "slow": 50
}

# ================ PURE PYTHON TA FUNCTIONS ================
def calculate_rsi_pure(prices: pd.Series, period: int = 14) -> pd.Series:
    """Pure Python RSI calculation"""
    delta = prices.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    
    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()
    
    for i in range(period, len(prices)):
        avg_gain.iloc[i] = (avg_gain.iloc[i-1] * (period - 1) + gain.iloc[i]) / period
        avg_loss.iloc[i] = (avg_loss.iloc[i-1] * (period - 1) + loss.iloc[i]) / period
    
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def calculate_macd_pure(prices: pd.Series, fast_period: int = 12, 
                       slow_period: int = 26, signal_period: int = 9) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """Pure Python MACD calculation"""
    ema_fast = prices.ewm(span=fast_period, adjust=False).mean()
    ema_slow = prices.ewm(span=slow_period, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal_period, adjust=False).mean()
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram

def calculate_ema(prices: pd.Series, period: int) -> pd.Series:
    """Calculate EMA - pure Python"""
    return prices.ewm(span=period, adjust=False).mean()

# ================ DATA STRUCTURES ================
@dataclass
class MarketStructure:
    """Market structure analysis"""
    trend: str
    higher_timeframe_aligned: bool
    key_support: float
    key_resistance: float
    structure_score: float
    swing_highs: List[float]
    swing_lows: List[float]
    breaker_blocks: List[Dict]

@dataclass
class OrderFlow:
    """Order flow & volume profile analysis"""
    volume_profile: Dict
    volume_spike: bool
    volume_ratio: float
    bid_ask_imbalance: float
    orderbook_depth: float
    accumulation_score: float
    large_transactions: int
    flow_score: float

@dataclass
class MomentumSignal:
    """Short-term momentum signals"""
    rsi_divergence: str
    rsi_value: float
    rsi_zone: str
    macd_signal: str
    macd_histogram: float
    candle_pattern: str
    pattern_strength: float
    momentum_score: float

@dataclass
class LiquidityZone:
    """Liquidity & stop hunt zones"""
    zone_type: str
    price_level: float
    distance_pct: float
    recently_tested: bool
    strength: float
    liquidation_cluster: Dict
    stop_hunt_potential: bool

@dataclass
class ConfluenceSetup:
    """Complete confluence setup"""
    signal_id: str
    symbol: str
    side: str
    
    market_structure: MarketStructure
    order_flow: OrderFlow
    momentum: MomentumSignal
    liquidity_zone: LiquidityZone
    
    entry_price: float
    entry_type: str
    entry_confidence: float
    
    stop_loss: float
    take_profit: float
    risk_pct: float
    reward_pct: float
    risk_reward: float
    
    confluence_score: float
    confluence_details: Dict
    conditions_met: List[str]
    
    expected_move_pct: float
    probability_score: float
    
    timeframe_used: str
    signal_timestamp: float
    
    # BTC alignment info (NEW)
    btc_alignment: Dict = None

# ================ PROFESSIONAL LOGGING ================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)8s | %(name)s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger("confluence_scanner")

# ================ BITCOIN STRUCTURE ANALYSIS ================
class BitcoinStructure:
    """Analyze Bitcoin as the primary market structure"""
    
    def __init__(self):
        self.current_structure = None
        self.last_update = 0
    
    async def analyze_bitcoin_structure(self, exchange) -> Dict:
        """Comprehensive Bitcoin structure analysis"""
        try:
            current_time = time.time()
            if (self.current_structure and 
                current_time - self.last_update < BTC_ANALYSIS_INTERVAL):
                return self.current_structure
            
            # Fetch BTC data across key timeframes
            btc_data = {}
            timeframes = {
                "DAILY": "1d",
                "4H": "4h",
                "1H": "1h",
                "15M": "15m"
            }
            
            # Fetch all timeframes in parallel
            tasks = []
            for tf_name, tf in timeframes.items():
                limit = 100 if tf_name == "DAILY" else 200
                tasks.append(self._fetch_btc_timeframe(exchange, tf, limit, tf_name))
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for tf_name, result in zip(timeframes.keys(), results):
                if isinstance(result, pd.DataFrame) and not result.empty:
                    btc_data[tf_name] = result
            
            # Analyze structure
            structure = self._analyze_multi_tf_structure(btc_data)
            
            # Update cache
            self.current_structure = structure
            self.last_update = current_time
            
            log.info(f"₿ BTC STRUCTURE: {structure['primary_trend']} | Strength: {structure['trend_strength']:.1f}/10")
            log.info(f"   Direction: {structure['direction']} | Regime: {structure['regime']}")
            
            return structure
            
        except Exception as e:
            log.error(f"BTC analysis error: {e}")
            return self._get_default_structure()
    
    async def _fetch_btc_timeframe(self, exchange, timeframe: str, limit: int, tf_name: str) -> pd.DataFrame:
        """Fetch BTC OHLCV data"""
        try:
            ohlcv = await exchange.fetch_ohlcv(
                "BTC/USDT",
                timeframe=timeframe,
                limit=limit,
                params={'type': 'spot'}
            )
            
            if ohlcv and len(ohlcv) >= 20:
                df = pd.DataFrame(
                    ohlcv,
                    columns=["timestamp", "open", "high", "low", "close", "volume"]
                )
                for col in ["open", "high", "low", "close", "volume"]:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                df = df.dropna()
                return df
            return pd.DataFrame()
            
        except Exception as e:
            log.debug(f"BTC {tf_name} fetch error: {e}")
            return pd.DataFrame()
    
    def _analyze_multi_tf_structure(self, btc_data: Dict) -> Dict:
        """Analyze Bitcoin structure across all timeframes"""
        structure = {
            "primary_trend": "NEUTRAL",
            "direction": "NEUTRAL",
            "trend_strength": 0.0,
            "regime": "RANGING",
            "key_levels": {},
            "alignment": {},
            "momentum": {},
            "timestamp": time.time()
        }
        
        try:
            # 1. Determine PRIMARY TREND (Daily)
            daily_trend = self._get_trend_direction(btc_data.get("DAILY", pd.DataFrame()), "DAILY")
            structure["primary_trend"] = daily_trend
            if daily_trend != "NEUTRAL":
                structure["trend_strength"] += 4.0
            
            # 2. Determine TRADING DIRECTION (4H + 1H)
            tf_4h_trend = self._get_trend_direction(btc_data.get("4H", pd.DataFrame()), "4H")
            tf_1h_trend = self._get_trend_direction(btc_data.get("1H", pd.DataFrame()), "1H")
            
            # Trading direction (what we actually trade)
            if tf_4h_trend == tf_1h_trend and tf_4h_trend != "NEUTRAL":
                structure["direction"] = tf_4h_trend
                structure["trend_strength"] += 3.0
            
            # 3. Check alignment
            trends = [daily_trend, tf_4h_trend, tf_1h_trend]
            alignment_score = sum(1 for t in trends if t == structure["direction"])
            structure["alignment"]["score"] = alignment_score / 3.0
            structure["alignment"]["all_aligned"] = alignment_score == 3
            
            # 4. Identify key levels
            if "DAILY" in btc_data:
                daily_df = btc_data["DAILY"]
                structure["key_levels"]["support"] = float(daily_df['low'].iloc[-20:].min())
                structure["key_levels"]["resistance"] = float(daily_df['high'].iloc[-20:].max())
                structure["key_levels"]["current"] = float(daily_df['close'].iloc[-1])
            
            # 5. Momentum analysis
            if "15M" in btc_data:
                momentum = self._analyze_momentum(btc_data["15M"])
                structure["momentum"] = momentum
            
            # 6. Determine market regime
            structure["regime"] = self._determine_regime(structure, btc_data)
            
            # 7. Final strength
            structure["trend_strength"] = min(structure["trend_strength"], 10.0)
            
            return structure
            
        except Exception as e:
            return self._get_default_structure()
    
    def _get_trend_direction(self, df: pd.DataFrame, timeframe: str) -> str:
        """Determine trend direction"""
        if df is None or df.empty or len(df) < 20:
            return "NEUTRAL"
        
        current_price = df['close'].iloc[-1]
        
        # EMA alignment
        ema_fast = calculate_ema(df['close'], 9).iloc[-1]
        ema_medium = calculate_ema(df['close'], 21).iloc[-1]
        ema_slow = calculate_ema(df['close'], 50).iloc[-1]
        
        # Check EMA alignment
        if ema_fast > ema_medium > ema_slow and current_price > ema_fast:
            return "BULLISH"
        elif ema_fast < ema_medium < ema_slow and current_price < ema_fast:
            return "BEARISH"
        
        # Check recent price action
        if len(df) >= 10:
            recent_return = (current_price - df['close'].iloc[-10]) / df['close'].iloc[-10] * 100
            if recent_return > 2.0:
                return "BULLISH"
            elif recent_return < -2.0:
                return "BEARISH"
        
        return "NEUTRAL"
    
    def _analyze_momentum(self, df: pd.DataFrame) -> Dict:
        """Analyze short-term momentum"""
        if df is None or df.empty or len(df) < 20:
            return {"rsi": 50, "macd": "NEUTRAL", "direction": "NEUTRAL"}
        
        # RSI
        rsi_values = calculate_rsi_pure(df['close'], period=14)
        current_rsi = float(rsi_values.iloc[-1]) if not rsi_values.empty else 50
        
        # MACD
        macd_line, signal_line, _ = calculate_macd_pure(df['close'])
        macd_signal = "NEUTRAL"
        if len(macd_line) > 1:
            if macd_line.iloc[-1] > signal_line.iloc[-1] and macd_line.iloc[-2] <= signal_line.iloc[-2]:
                macd_signal = "BULLISH_CROSS"
            elif macd_line.iloc[-1] < signal_line.iloc[-1] and macd_line.iloc[-2] >= signal_line.iloc[-2]:
                macd_signal = "BEARISH_CROSS"
        
        # Direction
        if "BULLISH" in macd_signal or current_rsi > 55:
            direction = "BULLISH"
        elif "BEARISH" in macd_signal or current_rsi < 45:
            direction = "BEARISH"
        else:
            direction = "NEUTRAL"
        
        return {
            "rsi": current_rsi,
            "macd": macd_signal,
            "direction": direction
        }
    
    def _determine_regime(self, structure: Dict, btc_data: Dict) -> str:
        """Determine current Bitcoin market regime"""
        trend_strength = structure["trend_strength"]
        direction = structure["direction"]
        
        if trend_strength >= 7.0:
            return f"STRONG_{direction}_TREND"
        elif trend_strength >= 5.0:
            return f"{direction}_TREND"
        elif trend_strength >= 3.0:
            return f"MILD_{direction}"
        
        # Check volatility
        if "15M" in btc_data:
            df = btc_data["15M"]
            if len(df) >= 10:
                volatility = df['close'].pct_change().std() * 100
                if volatility > 1.0:
                    return "HIGH_VOL_RANGE"
        
        return "LOW_VOL_RANGE"
    
    def _get_default_structure(self) -> Dict:
        return {
            "primary_trend": "NEUTRAL",
            "direction": "NEUTRAL",
            "trend_strength": 0.0,
            "regime": "RANGING",
            "key_levels": {"support": 0, "resistance": 0, "current": 0},
            "alignment": {"score": 0, "all_aligned": False},
            "momentum": {"rsi": 50, "macd": "NEUTRAL", "direction": "NEUTRAL"},
            "timestamp": time.time()
        }
    
    def should_trade_alts(self, btc_structure: Dict) -> bool:
        """Determine if we should trade altcoins based on BTC structure"""
        direction = btc_structure["direction"]
        strength = btc_structure["trend_strength"]
        regime = btc_structure["regime"]
        
        # MODIFIED: Allow trading when BTC is neutral (both sides allowed)
        if direction == "NEUTRAL":
            # Only require minimum strength, not specific direction
            return strength >= 2.0  # Lower threshold for neutral periods
        
        # Only trade alts when BTC has sufficient trend strength
        if strength < BTC_MIN_TREND_STRENGTH:
            return False
        
        # Avoid high volatility periods
        if "HIGH_VOL" in regime:
            return False
        
        return True
    
    def get_recommended_alt_side(self, btc_structure: Dict) -> str:
        """Get recommended side for altcoin trades based on BTC"""
        btc_direction = btc_structure["direction"]
        btc_strength = btc_structure["trend_strength"]
        btc_momentum = btc_structure["momentum"]["direction"]
        
        # BTC has strong direction - MUST follow
        if btc_direction == "BULLISH" and btc_strength >= 4.0:
            return "LONG"
        elif btc_direction == "BEARISH" and btc_strength >= 4.0:
            return "SHORT"
        
        # MODIFIED: BTC is neutral - allow BOTH sides
        elif btc_direction == "NEUTRAL":
            return "BOTH"  # Special value indicating both sides allowed
        
        # If BTC has weak direction, check momentum
        if btc_momentum == "BULLISH":
            return "LONG"
        elif btc_momentum == "BEARISH":
            return "SHORT"
        
        return "NEUTRAL"
    
    def calculate_btc_adjusted_score(self, alt_signal: ConfluenceSetup, btc_structure: Dict) -> float:
        """Adjust altcoin confluence score based on BTC alignment"""
        alt_side = alt_signal.side
        btc_recommended_side = self.get_recommended_alt_side(btc_structure)
        
        base_score = alt_signal.confluence_score
        
        # CRITICAL: Score multiplier based on BTC alignment
        # MODIFIED: Handle BOTH sides when BTC is neutral
        if btc_recommended_side == "BOTH":  
            # BTC neutral - no penalty or bonus (1.0x multiplier)
            alignment_multiplier = 1.0
            
        elif alt_side == btc_recommended_side:
            # STRONG ALIGNMENT - major bonus
            alignment_multiplier = 1.3
            
            # Extra bonus if BTC trend is strong
            if btc_structure["trend_strength"] >= 7.0:
                alignment_multiplier = 1.5
            
        elif btc_recommended_side == "NEUTRAL":
            # BTC has no strong opinion - neutral multiplier
            alignment_multiplier = 1.0
            
        else:
            # COUNTER-BTC TRADE - heavy penalty
            alignment_multiplier = 0.5
        
        # Apply multiplier
        adjusted_score = base_score * alignment_multiplier
        return min(adjusted_score, 10.0)

# ================ BTC-CENTRIC CONFLUENCE SCANNER ================
class BTCConfluenceScanner:
    """BTC-centric confluence scanner - Bitcoin structure determines altcoin trades"""
    
    class SignalManager:
        """Manage signals with BTC-based filtering"""
        
        def __init__(self):
            self.active_signals = {}
            self.signal_states = {}
            self.confluence_history = {}
            self.consecutive_failures = {}
            self.symbol_blacklist = {}
            
        def should_generate_signal(self, symbol: str, new_score: float, side: str) -> bool:
            """Check if new signal has significantly better confluence"""
            if symbol in self.symbol_blacklist:
                if time.time() < self.symbol_blacklist[symbol]:
                    return False
                else:
                    del self.symbol_blacklist[symbol]
            
            if symbol not in self.active_signals:
                return True
            
            signal_id = self.active_signals[symbol]
            if signal_id not in self.signal_states:
                return True
            
            state = self.signal_states[signal_id]
            if state.get("status") == "CLOSED":
                return True
            
            old_score = state.get("confluence_score", 0)
            if new_score > old_score * 1.15:
                return True
            
            return False
        
        def register_signal(self, signal: ConfluenceSetup):
            """Register new confluence signal"""
            symbol = signal.symbol
            side = signal.side
            
            # Clear old if exists
            if symbol in self.active_signals:
                old_id = self.active_signals[symbol]
                if old_id in self.signal_states:
                    del self.signal_states[old_id]
            
            # Register new
            self.active_signals[symbol] = signal.signal_id
            self.signal_states[signal.signal_id] = {
                "symbol": symbol,
                "side": side,
                "confluence_score": signal.confluence_score,
                "status": "PENDING",
                "timestamp": signal.signal_timestamp
            }
            
            # Track confluence history
            if symbol not in self.confluence_history:
                self.confluence_history[symbol] = deque(maxlen=10)
            self.confluence_history[symbol].append(signal.confluence_score)
            
            # Reset failure count for this side
            if symbol in self.consecutive_failures:
                self.consecutive_failures[symbol][side] = 0
            
            log.debug(f"Registered confluence signal {signal.signal_id[:8]} for {symbol} {side}")
        
        def update_signal_status(self, signal_id: str, status: str, result: str = None):
            """Update signal status with result"""
            if signal_id in self.signal_states:
                state = self.signal_states[signal_id]
                state["status"] = status
                
                # Track failures if closed with loss
                if status == "CLOSED" and result == "LOSS":
                    symbol = state["symbol"]
                    side = state["side"]
                    
                    if symbol not in self.consecutive_failures:
                        self.consecutive_failures[symbol] = {"LONG": 0, "SHORT": 0}
                    
                    self.consecutive_failures[symbol][side] += 1
                    
                    # Blacklist after 3 consecutive failures
                    if self.consecutive_failures[symbol][side] >= 3:
                        self.symbol_blacklist[symbol] = time.time() + (4 * 3600)
                        log.warning(f"{symbol} {side}: 3+ consecutive losses - blacklisted for 4 hours")
                    
                    # Reset opposite side
                    opposite_side = "SHORT" if side == "LONG" else "LONG"
                    self.consecutive_failures[symbol][opposite_side] = 0
                
                log.debug(f"Signal {signal_id[:8]} → {status}" + (f" ({result})" if result else ""))
        
        def get_consecutive_failures(self, symbol: str, side: str) -> int:
            """Get consecutive failures for symbol/side"""
            if symbol in self.consecutive_failures:
                return self.consecutive_failures[symbol].get(side, 0)
            return 0
        
        def is_blacklisted(self, symbol: str) -> bool:
            """Check if symbol is currently blacklisted"""
            if symbol in self.symbol_blacklist:
                if time.time() < self.symbol_blacklist[symbol]:
                    return True
                else:
                    del self.symbol_blacklist[symbol]
            return False
        
        def cleanup_old_signals(self):
            """Clean up old closed signals"""
            current_time = time.time()
            to_remove = []
            
            for signal_id, state in list(self.signal_states.items()):
                if state.get("status") == "CLOSED":
                    age = current_time - state.get("timestamp", 0)
                    if age > 3600:
                        to_remove.append(signal_id)
            
            for signal_id in to_remove:
                symbol = self.signal_states[signal_id]["symbol"]
                if self.active_signals.get(symbol) == signal_id:
                    del self.active_signals[symbol]
                del self.signal_states[signal_id]
    
    def __init__(self):
        self.signal_manager = self.SignalManager()
        self.bitcoin_structure = BitcoinStructure()
        self.btc_structure = None
        self.daily_stats = {
            "scans": 0,
            "pairs_analyzed": 0,
            "confluence_signals": 0,
            "high_quality_signals": 0,
            "rejected_low_confluence": 0,
            "rejected_no_alignment": 0,
            "rejected_counter_trend": 0,
            "rejected_blacklisted": 0,
            # BTC-centric stats
            "rejected_counter_btc": 0,
            "rejected_btc_neutral": 0,
            "btc_aligned_signals": 0,
            "btc_neutral_signals": 0,  # NEW: Signals during BTC neutral
            "btc_direction": "NEUTRAL"
        }
    
    # ========== MARKET STRUCTURE ANALYSIS ==========
    
    def analyze_market_structure(self, df_daily: pd.DataFrame, df_4h: pd.DataFrame, 
                                df_1h: pd.DataFrame) -> MarketStructure:
        """Analyze market structure across multiple timeframes"""
        try:
            if not self._validate_dataframe(df_daily, 20):
                return self._get_default_structure()
            if not self._validate_dataframe(df_4h, 20):
                return self._get_default_structure()
            if not self._validate_dataframe(df_1h, 20):
                return self._get_default_structure()
            
            # 1. Determine primary trend (Daily)
            daily_trend = self._determine_trend(df_daily, "DAILY")
            
            # 2. Check HTF alignment (4H aligns with Daily)
            htf_aligned = self._check_htf_alignment(df_daily, df_4h)
            
            # 3. Identify key levels (4H)
            key_support, key_resistance = self._identify_key_levels(df_4h)
            
            # 4. Find swing highs/lows (1H)
            swing_highs, swing_lows = self._find_swing_points(df_1h)
            
            # 5. Find order blocks/FVGs (1H)
            breaker_blocks = self._find_breaker_blocks(df_1h)
            
            # 6. Calculate structure score
            structure_score = self._calculate_structure_score(
                daily_trend, htf_aligned, swing_highs, swing_lows
            )
            
            return MarketStructure(
                trend=daily_trend,
                higher_timeframe_aligned=htf_aligned,
                key_support=key_support,
                key_resistance=key_resistance,
                structure_score=structure_score,
                swing_highs=swing_highs,
                swing_lows=swing_lows,
                breaker_blocks=breaker_blocks
            )
            
        except Exception as e:
            log.error(f"Market structure error: {e}")
            return self._get_default_structure()
    
    def _validate_dataframe(self, df: pd.DataFrame, min_rows: int) -> bool:
        """Validate dataframe for analysis"""
        if df is None or df.empty:
            return False
        if len(df) < min_rows:
            return False
        if any(df[col].isna().any() for col in ['open', 'high', 'low', 'close', 'volume']):
            return False
        return True
    
    def _determine_trend(self, df: pd.DataFrame, timeframe: str) -> str:
        """Determine trend direction"""
        try:
            if len(df) < 50:
                return "RANGING"
            
            ema_fast = calculate_ema(df['close'], 9).iloc[-1]
            ema_medium = calculate_ema(df['close'], 21).iloc[-1]
            ema_slow = calculate_ema(df['close'], 50).iloc[-1]
            
            if ema_fast > ema_medium > ema_slow:
                return "BULLISH"
            elif ema_fast < ema_medium < ema_slow:
                return "BEARISH"
            
            current_price = df['close'].iloc[-1]
            if current_price > ema_medium:
                return "BULLISH"
            elif current_price < ema_medium:
                return "BEARISH"
            
            return "RANGING"
            
        except Exception as e:
            return "RANGING"
    
    def _check_htf_alignment(self, df_higher: pd.DataFrame, df_lower: pd.DataFrame) -> bool:
        """Check if higher and lower timeframes align"""
        try:
            if len(df_higher) < 20 or len(df_lower) < 20:
                return False
            
            higher_trend = self._determine_trend(df_higher, "HTF")
            lower_trend = self._determine_trend(df_lower, "LTF")
            
            if higher_trend == "RANGING" or lower_trend == "RANGING":
                return False
            
            return higher_trend == lower_trend
            
        except Exception as e:
            return False
    
    def _identify_key_levels(self, df: pd.DataFrame) -> Tuple[float, float]:
        """Identify key support and resistance levels"""
        try:
            if len(df) < 30:
                return 0.0, 0.0
            
            recent_highs = df['high'].iloc[-30:].nlargest(3).values
            recent_lows = df['low'].iloc[-30:].nsmallest(3).values
            
            key_resistance = np.mean(recent_highs) if len(recent_highs) > 0 else 0.0
            key_support = np.mean(recent_lows) if len(recent_lows) > 0 else 0.0
            
            return float(key_support), float(key_resistance)
            
        except Exception as e:
            return 0.0, 0.0
    
    def _find_swing_points(self, df: pd.DataFrame) -> Tuple[List[float], List[float]]:
        """Find recent swing highs and lows"""
        try:
            if len(df) < 20:
                return [], []
            
            swing_highs = []
            swing_lows = []
            
            for i in range(2, len(df) - 2):
                if i < 5:
                    continue
                
                high = df['high'].iloc[i]
                low = df['low'].iloc[i]
                
                # Check for swing high
                if (high > df['high'].iloc[i-1] and 
                    high > df['high'].iloc[i-2] and
                    high > df['high'].iloc[i+1] and
                    high > df['high'].iloc[i+2]):
                    swing_highs.append(float(high))
                
                # Check for swing low
                if (low < df['low'].iloc[i-1] and 
                    low < df['low'].iloc[i-2] and
                    low < df['low'].iloc[i+1] and
                    low < df['low'].iloc[i+2]):
                    swing_lows.append(float(low))
            
            return swing_highs[-3:], swing_lows[-3:]
            
        except Exception as e:
            return [], []
    
    def _find_breaker_blocks(self, df: pd.DataFrame) -> List[Dict]:
        """Find order blocks / fair value gaps"""
        try:
            if len(df) < 10:
                return []
            
            blocks = []
            
            for i in range(1, len(df) - 1):
                current = df.iloc[i]
                prev = df.iloc[i-1]
                
                # Bearish order block
                if (prev['close'] > prev['open'] and
                    current['close'] < current['open'] and
                    current['low'] < prev['low']):
                    
                    block = {
                        "type": "BEARISH_OB",
                        "high": float(prev['high']),
                        "low": float(prev['low']),
                        "index": i
                    }
                    blocks.append(block)
                
                # Bullish order block
                if (prev['close'] < prev['open'] and
                    current['close'] > current['open'] and
                    current['high'] > prev['high']):
                    
                    block = {
                        "type": "BULLISH_OB",
                        "high": float(prev['high']),
                        "low": float(prev['low']),
                        "index": i
                    }
                    blocks.append(block)
            
            return blocks[-5:]
            
        except Exception as e:
            return []
    
    def _calculate_structure_score(self, trend: str, aligned: bool, 
                                  swing_highs: List, swing_lows: List) -> float:
        """Calculate market structure quality score"""
        score = 0.0
        
        if trend != "RANGING":
            score += 0.3
        
        if aligned:
            score += 0.3
        elif trend != "RANGING":
            score -= 0.2
        
        if len(swing_highs) >= 2 and len(swing_lows) >= 2:
            score += 0.4
        
        return max(0.0, min(score, 1.0))
    
    def _get_default_structure(self) -> MarketStructure:
        return MarketStructure(
            trend="RANGING",
            higher_timeframe_aligned=False,
            key_support=0.0,
            key_resistance=0.0,
            structure_score=0.0,
            swing_highs=[],
            swing_lows=[],
            breaker_blocks=[]
        )
    
    # ========== ORDER FLOW ANALYSIS ==========
    
    def analyze_order_flow(self, df_15m: pd.DataFrame, current_price: float) -> OrderFlow:
        """Analyze order flow and volume profile"""
        try:
            if not self._validate_dataframe(df_15m, 30):
                return self._get_default_order_flow()
            
            volume_profile = self._analyze_volume_profile(df_15m)
            volume_spike, volume_ratio = self._detect_volume_spike(df_15m)
            bid_ask_imbalance = self._estimate_bid_ask_imbalance(df_15m)
            orderbook_depth = self._estimate_orderbook_depth(df_15m)
            accumulation_score = self._calculate_accumulation_score(df_15m)
            large_transactions = self._count_large_transactions(df_15m)
            
            flow_score = self._calculate_flow_score(
                volume_spike, volume_ratio, bid_ask_imbalance,
                orderbook_depth, accumulation_score
            )
            
            return OrderFlow(
                volume_profile=volume_profile,
                volume_spike=volume_spike,
                volume_ratio=volume_ratio,
                bid_ask_imbalance=bid_ask_imbalance,
                orderbook_depth=orderbook_depth,
                accumulation_score=accumulation_score,
                large_transactions=large_transactions,
                flow_score=flow_score
            )
            
        except Exception as e:
            log.error(f"Order flow error: {e}")
            return self._get_default_order_flow()
    
    def _analyze_volume_profile(self, df: pd.DataFrame) -> Dict:
        """Analyze volume profile for value areas"""
        try:
            if len(df) < 20:
                return {"high_volume_nodes": [], "low_volume_gaps": []}
            
            price_bins = np.linspace(df['low'].min(), df['high'].max(), 20)
            volume_by_price = []
            
            for i in range(len(price_bins) - 1):
                low_bin = price_bins[i]
                high_bin = price_bins[i+1]
                
                mask = (df['low'] >= low_bin) & (df['high'] <= high_bin)
                volume_in_range = df.loc[mask, 'volume'].sum()
                
                volume_by_price.append({
                    "price_range": (float(low_bin), float(high_bin)),
                    "volume": float(volume_in_range)
                })
            
            volumes = [item["volume"] for item in volume_by_price]
            if volumes:
                avg_volume = np.mean(volumes)
                high_volume_nodes = [
                    item for item in volume_by_price 
                    if item["volume"] > avg_volume * 1.5
                ]
            else:
                high_volume_nodes = []
            
            return {
                "high_volume_nodes": high_volume_nodes[:3],
                "low_volume_gaps": []
            }
            
        except Exception as e:
            return {"high_volume_nodes": [], "low_volume_gaps": []}
    
    def _detect_volume_spike(self, df: pd.DataFrame) -> Tuple[bool, float]:
        """Detect volume spikes"""
        try:
            if len(df) < 10:
                return False, 1.0
            
            recent_volume = df['volume'].iloc[-3:].mean()
            avg_volume = df['volume'].iloc[-20:].mean()
            
            if avg_volume > 0:
                ratio = recent_volume / avg_volume
                spike = ratio >= 2.0
                return spike, float(ratio)
            
            return False, 1.0
            
        except Exception as e:
            return False, 1.0
    
    def _estimate_bid_ask_imbalance(self, df: pd.DataFrame) -> float:
        """Estimate bid/ask imbalance from price action"""
        try:
            if len(df) < 10:
                return 0.0
            
            recent = df.iloc[-5:]
            closes = recent['close'].values
            opens = recent['open'].values
            
            bullish = sum(closes > opens)
            bearish = sum(closes < opens)
            
            total = bullish + bearish
            if total > 0:
                imbalance = (bullish - bearish) / total
                return float(imbalance)
            
            return 0.0
            
        except Exception as e:
            return 0.0
    
    def _estimate_orderbook_depth(self, df: pd.DataFrame) -> float:
        """Estimate orderbook depth from volatility"""
        try:
            if len(df) < 20:
                return 0.5
            
            recent_volatility = df['close'].pct_change().std() * 100
            avg_volatility = df['close'].pct_change().iloc[-50:].std() * 100
            
            if avg_volatility > 0:
                depth_ratio = 1.0 - min(recent_volatility / avg_volatility, 2.0) / 2.0
                return float(depth_ratio)
            
            return 0.5
            
        except Exception as e:
            return 0.5
    
    def _calculate_accumulation_score(self, df: pd.DataFrame) -> float:
        """Calculate accumulation/distribution score"""
        try:
            if len(df) < 30:
                return 0.5
            
            price_change = df['close'].iloc[-1] - df['close'].iloc[-10]
            volume_change = df['volume'].iloc[-10:].mean() / df['volume'].iloc[-30:-10].mean()
            
            if price_change > 0 and volume_change > 1.2:
                return 0.8
            elif price_change < 0 and volume_change < 0.8:
                return 0.6
            
            return 0.5
            
        except Exception as e:
            return 0.5
    
    def _count_large_transactions(self, df: pd.DataFrame) -> int:
        """Count large transactions (simulated)"""
        try:
            if len(df) < 10:
                return 0
            
            avg_volume = df['volume'].mean()
            large_tx_count = sum(df['volume'] > avg_volume * 3)
            
            return min(large_tx_count, 10)
            
        except Exception as e:
            return 0
    
    def _calculate_flow_score(self, volume_spike: bool, volume_ratio: float,
                             imbalance: float, depth: float, accumulation: float) -> float:
        """Calculate overall flow score"""
        weights = [0.25, 0.20, 0.25, 0.15, 0.15]
        
        spike_score = 1.0 if volume_spike else 0.5
        volume_score = min(volume_ratio / 3.0, 1.0)
        imbalance_score = abs(imbalance)
        depth_score = depth
        accum_score = accumulation
        
        factors = [spike_score, volume_score, imbalance_score, depth_score, accum_score]
        
        return float(np.average(factors, weights=weights))
    
    def _get_default_order_flow(self) -> OrderFlow:
        return OrderFlow(
            volume_profile={},
            volume_spike=False,
            volume_ratio=1.0,
            bid_ask_imbalance=0.0,
            orderbook_depth=0.5,
            accumulation_score=0.5,
            large_transactions=0,
            flow_score=0.5
        )
    
    # ========== MOMENTUM ANALYSIS ==========
    
    def analyze_momentum(self, df_15m: pd.DataFrame, df_5m: pd.DataFrame) -> MomentumSignal:
        """Analyze short-term momentum signals"""
        try:
            if not self._validate_dataframe(df_15m, 30) or not self._validate_dataframe(df_5m, 30):
                return self._get_default_momentum()
            
            rsi_divergence, rsi_value, rsi_zone = self._analyze_rsi_pure(df_15m)
            macd_signal, macd_histogram = self._analyze_macd_pure(df_15m)
            candle_pattern, pattern_strength = self._analyze_candle_patterns(df_5m)
            
            momentum_score = self._calculate_momentum_score(
                rsi_divergence, rsi_zone, macd_signal, candle_pattern
            )
            
            return MomentumSignal(
                rsi_divergence=rsi_divergence,
                rsi_value=rsi_value,
                rsi_zone=rsi_zone,
                macd_signal=macd_signal,
                macd_histogram=macd_histogram,
                candle_pattern=candle_pattern,
                pattern_strength=pattern_strength,
                momentum_score=momentum_score
            )
            
        except Exception as e:
            log.error(f"Momentum error: {e}")
            return self._get_default_momentum()
    
    def _analyze_rsi_pure(self, df: pd.DataFrame) -> Tuple[str, float, str]:
        """Analyze RSI for divergence and zones"""
        try:
            if len(df) < 30:
                return "NONE", 50.0, "NEUTRAL"
            
            prices = df['close']
            rsi_values = calculate_rsi_pure(prices, period=RSI_PERIOD)
            
            if len(rsi_values) < 5 or np.isnan(rsi_values.iloc[-1]):
                return "NONE", 50.0, "NEUTRAL"
            
            current_rsi = float(rsi_values.iloc[-1])
            
            if current_rsi <= RSI_OVERSOLD:
                rsi_zone = "OVERSOLD"
            elif current_rsi >= RSI_OVERBOUGHT:
                rsi_zone = "OVERBOUGHT"
            else:
                rsi_zone = "NEUTRAL"
            
            rsi_divergence = "NONE"
            
            if len(rsi_values) >= 10:
                recent_rsi = rsi_values.iloc[-5:].values
                recent_prices = prices.iloc[-5:].values
                
                if (recent_rsi[-1] > recent_rsi[-3] and
                    recent_prices[-1] < recent_prices[-3]):
                    rsi_divergence = "BULLISH_HIDDEN"
                
                elif (recent_rsi[-1] < recent_rsi[-3] and
                      recent_prices[-1] > recent_prices[-3]):
                    rsi_divergence = "BEARISH_HIDDEN"
            
            return rsi_divergence, current_rsi, rsi_zone
            
        except Exception as e:
            return "NONE", 50.0, "NEUTRAL"
    
    def _analyze_macd_pure(self, df: pd.DataFrame) -> Tuple[str, float]:
        """Analyze MACD signals"""
        try:
            if len(df) < 35:
                return "NONE", 0.0
            
            prices = df['close'].values
            macd_line, signal_line, hist = calculate_macd_pure(
                df['close'],
                fast_period=12,
                slow_period=26,
                signal_period=9
            )
            
            if len(macd_line) < 2 or np.isnan(macd_line.iloc[-1]):
                return "NONE", 0.0
            
            current_hist = float(hist.iloc[-1])
            prev_hist = float(hist.iloc[-2]) if len(hist) > 1 else 0.0
            
            current_macd = float(macd_line.iloc[-1])
            current_signal = float(signal_line.iloc[-1])
            prev_macd = float(macd_line.iloc[-2]) if len(macd_line) > 1 else 0.0
            prev_signal = float(signal_line.iloc[-2]) if len(signal_line) > 1 else 0.0
            
            macd_signal = "NONE"
            
            if prev_macd < prev_signal and current_macd > current_signal:
                macd_signal = "BULLISH_CROSS"
            elif prev_macd > prev_signal and current_macd < current_signal:
                macd_signal = "BEARISH_CROSS"
            elif prev_hist < 0 and current_hist > 0:
                macd_signal = "BULLISH_FLIP"
            elif prev_hist > 0 and current_hist < 0:
                macd_signal = "BEARISH_FLIP"
            
            return macd_signal, current_hist
            
        except Exception as e:
            return "NONE", 0.0
    
    def _analyze_candle_patterns(self, df: pd.DataFrame) -> Tuple[str, float]:
        """Analyze candlestick patterns"""
        try:
            if len(df) < 5:
                return "NONE", 0.0
            
            current = df.iloc[-1]
            prev1 = df.iloc[-2]
            
            # Bullish engulfing
            if (prev1['close'] < prev1['open'] and
                current['close'] > current['open'] and
                current['close'] > prev1['open'] and
                current['open'] < prev1['close']):
                return "BULLISH_ENGULFING", 0.8
            
            # Bearish engulfing
            if (prev1['close'] > prev1['open'] and
                current['close'] < current['open'] and
                current['close'] < prev1['open'] and
                current['open'] > prev1['close']):
                return "BEARISH_ENGULFING", 0.8
            
            # Hammer
            if (current['close'] > current['open'] and
                (current['low'] - min(current['open'], current['close'])) >
                (abs(current['close'] - current['open']) * 2) and
                (current['high'] - max(current['open'], current['close'])) <
                abs(current['close'] - current['open'])):
                return "HAMMER", 0.7
            
            # Shooting star
            if (current['close'] < current['open'] and
                (current['high'] - max(current['open'], current['close'])) >
                (abs(current['close'] - current['open']) * 2) and
                (min(current['open'], current['close']) - current['low']) <
                abs(current['close'] - current['open'])):
                return "SHOOTING_STAR", 0.7
            
            # Inside bar
            if (current['high'] < prev1['high'] and
                current['low'] > prev1['low']):
                return "INSIDE_BAR", 0.6
            
            return "NONE", 0.0
            
        except Exception as e:
            return "NONE", 0.0
    
    def _calculate_momentum_score(self, rsi_div: str, rsi_zone: str,
                                 macd_signal: str, candle_pattern: str) -> float:
        """Calculate momentum score"""
        score = 0.0
        
        if rsi_div != "NONE":
            score += 0.4
        elif rsi_zone in ["OVERSOLD", "OVERBOUGHT"]:
            score += 0.2
        
        if macd_signal != "NONE":
            score += 0.3
        
        if candle_pattern != "NONE":
            score += 0.3
        
        return min(score, 1.0)
    
    def _get_default_momentum(self) -> MomentumSignal:
        return MomentumSignal(
            rsi_divergence="NONE",
            rsi_value=50.0,
            rsi_zone="NEUTRAL",
            macd_signal="NONE",
            macd_histogram=0.0,
            candle_pattern="NONE",
            pattern_strength=0.0,
            momentum_score=0.0
        )
    
    # ========== LIQUIDITY ZONE ANALYSIS ==========
    
    def analyze_liquidity_zones(self, df_4h: pd.DataFrame, df_1h: pd.DataFrame,
                               current_price: float) -> LiquidityZone:
        """Analyze liquidity zones for stop hunts"""
        try:
            if not self._validate_dataframe(df_4h, 20) or not self._validate_dataframe(df_1h, 20):
                return self._get_default_liquidity_zone()
            
            best_zone = self._identify_liquidity_zone(df_4h, df_1h, current_price)
            
            if best_zone["price_level"] > 0:
                distance_pct = abs(current_price - best_zone["price_level"]) / current_price * 100
            else:
                distance_pct = 0.0
            
            recently_tested = self._check_recent_test(df_1h, best_zone["price_level"])
            liquidation_cluster = self._estimate_liquidations(df_1h, best_zone["price_level"])
            
            stop_hunt_potential = self._assess_stop_hunt_potential(
                best_zone["zone_type"], distance_pct, liquidation_cluster
            )
            
            return LiquidityZone(
                zone_type=best_zone["zone_type"],
                price_level=best_zone["price_level"],
                distance_pct=float(distance_pct),
                recently_tested=recently_tested,
                strength=best_zone["strength"],
                liquidation_cluster=liquidation_cluster,
                stop_hunt_potential=stop_hunt_potential
            )
            
        except Exception as e:
            log.error(f"Liquidity zone error: {e}")
            return self._get_default_liquidity_zone()
    
    def _identify_liquidity_zone(self, df_4h: pd.DataFrame, df_1h: pd.DataFrame,
                                current_price: float) -> Dict:
        """Identify the strongest liquidity zone"""
        zones = []
        
        try:
            # Recent swing high/low (4H)
            recent_high_4h = df_4h['high'].iloc[-20:].max()
            recent_low_4h = df_4h['low'].iloc[-20:].min()
            
            if abs(current_price - recent_high_4h) / recent_high_4h < 0.01:
                zones.append({
                    "zone_type": "EQ_HIGH",
                    "price_level": float(recent_high_4h),
                    "strength": 0.8
                })
            
            if abs(current_price - recent_low_4h) / recent_low_4h < 0.01:
                zones.append({
                    "zone_type": "EQ_LOW",
                    "price_level": float(recent_low_4h),
                    "strength": 0.8
                })
            
            # Previous liquidity sweeps (1H)
            if len(df_1h) >= 10:
                for i in range(5, len(df_1h) - 1):
                    candle = df_1h.iloc[i]
                    prev_high = df_1h['high'].iloc[i-5:i].max()
                    prev_low = df_1h['low'].iloc[i-5:i].min()
                    
                    # Sweep high
                    if candle['high'] > prev_high * 1.005:
                        zones.append({
                            "zone_type": "SWEEP_HIGH",
                            "price_level": float(candle['high']),
                            "strength": 0.9
                        })
                    
                    # Sweep low
                    if candle['low'] < prev_low * 0.995:
                        zones.append({
                            "zone_type": "SWEEP_LOW",
                            "price_level": float(candle['low']),
                            "strength": 0.9
                        })
            
            if zones:
                zones.sort(key=lambda x: (
                    x["strength"],
                    -abs(current_price - x["price_level"]) / current_price
                ), reverse=True)
                return zones[0]
            
        except Exception as e:
            pass
        
        return {"zone_type": "NONE", "price_level": 0.0, "strength": 0.0}
    
    def _check_recent_test(self, df: pd.DataFrame, price_level: float) -> bool:
        """Check if price level was recently tested"""
        try:
            if price_level == 0 or len(df) < 5:
                return False
            
            recent_candles = df.iloc[-5:]
            for _, candle in recent_candles.iterrows():
                if candle['low'] <= price_level <= candle['high']:
                    return True
            
            return False
            
        except Exception as e:
            return False
    
    def _estimate_liquidations(self, df: pd.DataFrame, price_level: float) -> Dict:
        """Estimate liquidation levels (simulated)"""
        try:
            if price_level == 0 or len(df) < 20:
                return {"longs": 0, "shorts": 0, "total": 0}
            
            volatility = df['close'].pct_change().std() * 100
            recent_high = df['high'].iloc[-10:].max()
            recent_low = df['low'].iloc[-10:].min()
            
            if abs(price_level - recent_high) / recent_high < 0.01:
                return {"longs": 50, "shorts": 150, "total": 200}
            elif abs(price_level - recent_low) / recent_low < 0.01:
                return {"longs": 150, "shorts": 50, "total": 200}
            else:
                return {"longs": 100, "shorts": 100, "total": 200}
                
        except Exception as e:
            return {"longs": 0, "shorts": 0, "total": 0}
    
    def _assess_stop_hunt_potential(self, zone_type: str, distance_pct: float,
                                   liquidations: Dict) -> bool:
        """Assess stop hunt potential"""
        if zone_type == "NONE":
            return False
        
        if distance_pct < 1.0 and liquidations.get("total", 0) > 100:
            return True
        
        if "SWEEP" in zone_type:
            return True
        
        return False
    
    def _get_default_liquidity_zone(self) -> LiquidityZone:
        return LiquidityZone(
            zone_type="NONE",
            price_level=0.0,
            distance_pct=0.0,
            recently_tested=False,
            strength=0.0,
            liquidation_cluster={},
            stop_hunt_potential=False
        )
    
    # ========== CONFLUENCE SIGNAL GENERATION (BTC-CENTRIC) ==========
    
    async def generate_confluence_signal(self, multi_tf_data: Dict[str, pd.DataFrame],
                                       symbol: str, exchange=None) -> Optional[ConfluenceSetup]:
        """
        Generate BTC-CENTRIC confluence signal
        Bitcoin structure is PRIMARY, altcoin must align
        When BTC is neutral, both LONG and SHORT allowed
        """
        
        # === STEP 1: ANALYZE BITCOIN FIRST (CRITICAL) ===
        if exchange and (self.btc_structure is None or time.time() - self.btc_structure.get("timestamp", 0) > BTC_ANALYSIS_INTERVAL):
            self.btc_structure = await self.bitcoin_structure.analyze_bitcoin_structure(exchange)
            self.daily_stats["btc_direction"] = self.btc_structure["direction"]
        
        # MODIFIED: Allow scanning even when BTC is neutral
        if not self.btc_structure:
            self.daily_stats["rejected_btc_neutral"] += 1
            log.debug(f"{symbol}: BTC structure not available")
            return None
        
        # Check if we should trade alts in this BTC regime
        if not self.bitcoin_structure.should_trade_alts(self.btc_structure):
            self.daily_stats["rejected_btc_neutral"] += 1
            log.debug(f"{symbol}: BTC regime {self.btc_structure['regime']} not suitable for alts")
            return None
        
        # Get BTC-recommended side
        btc_recommended_side = self.bitcoin_structure.get_recommended_alt_side(self.btc_structure)
        
        # === STEP 2: Analyze altcoin (your existing analysis) ===
        try:
            df_daily = multi_tf_data.get("DAILY")
            df_4h = multi_tf_data.get("4H")
            df_1h = multi_tf_data.get("1H")
            df_15m = multi_tf_data.get("15M")
            df_5m = multi_tf_data.get("5M")
            
            required_minimums = {
                "DAILY": 30,
                "4H": 40,
                "1H": 50,
                "15M": 60,
                "5M": 50
            }
            
            for tf_name, df in [("DAILY", df_daily), ("4H", df_4h), ("1H", df_1h),
                               ("15M", df_15m), ("5M", df_5m)]:
                min_len = required_minimums[tf_name]
                if df is None or df.empty or len(df) < min_len:
                    log.debug(f"{symbol}: Insufficient {tf_name} data")
                    return None
                
                if not self._validate_dataframe(df, 15):
                    log.debug(f"{symbol}: Invalid data in {tf_name}")
                    return None
            
            current_price = df_5m['close'].iloc[-1]
            
            # === STEP 3: Analyze altcoin components ===
            market_structure = self.analyze_market_structure(df_daily, df_4h, df_1h)
            
            # Check for blacklisted symbol
            if self.signal_manager.is_blacklisted(symbol):
                self.daily_stats["rejected_blacklisted"] += 1
                log.debug(f"{symbol}: Currently blacklisted")
                return None
            
            order_flow = self.analyze_order_flow(df_15m, current_price)
            momentum = self.analyze_momentum(df_15m, df_5m)
            liquidity_zone = self.analyze_liquidity_zones(df_4h, df_1h, current_price)
            
            # === STEP 4: Determine altcoin side WITH BTC CONSTRAINT ===
            # First, get the altcoin's natural side from confluence
            alt_side = self._determine_trade_side_original(
                market_structure, momentum, liquidity_zone, symbol
            )
            
            if not alt_side:
                log.debug(f"{symbol}: No clear altcoin side from confluence")
                return None
            
            # === STEP 5: MODIFIED BTC ALIGNMENT RULES ===
            # BTC neutral - allow both LONG and SHORT
            if btc_recommended_side == "BOTH":
                self.daily_stats["btc_neutral_signals"] += 1
                log.debug(f"{symbol}: BTC neutral - allowing both sides ({alt_side})")
                
            # BTC has clear direction - must align
            elif alt_side != btc_recommended_side:
                self.daily_stats["rejected_counter_btc"] += 1
                log.debug(f"{symbol}: Rejected - {alt_side} vs BTC {btc_recommended_side}")
                return None
            
            else:
                # Perfect BTC alignment - proceed
                self.daily_stats["btc_aligned_signals"] += 1
                log.debug(f"{symbol}: Perfect BTC alignment - {alt_side}")
            
            # === STEP 6: Check consecutive failures ===
            consecutive_failures = self.signal_manager.get_consecutive_failures(symbol, alt_side)
            if consecutive_failures >= 2:
                log.debug(f"{symbol} {alt_side}: Skipping due to {consecutive_failures} consecutive failures")
                return None
            
            # === STEP 7: Calculate confluence score ===
            confluence_score, confluence_details = self.calculate_confluence_score(
                market_structure, order_flow, momentum, liquidity_zone, alt_side
            )
            
            # Apply BTC adjustment to score
            btc_adjusted_score = self.bitcoin_structure.calculate_btc_adjusted_score(
                ConfluenceSetup(
                    signal_id="temp",
                    symbol=symbol,
                    side=alt_side,
                    confluence_score=confluence_score,
                    # Other fields are not needed for score adjustment
                    market_structure=market_structure,
                    order_flow=order_flow,
                    momentum=momentum,
                    liquidity_zone=liquidity_zone,
                    entry_price=current_price,
                    entry_type="",
                    entry_confidence=0,
                    stop_loss=0,
                    take_profit=0,
                    risk_pct=0,
                    reward_pct=0,
                    risk_reward=0,
                    confluence_details={},
                    conditions_met=[],
                    expected_move_pct=0,
                    probability_score=0,
                    timeframe_used="",
                    signal_timestamp=0
                ),
                self.btc_structure
            )
            
            # CRITICAL: Minimum confluence score after BTC adjustment
            if btc_adjusted_score < MIN_CONFLUENCE_SCORE:
                self.daily_stats["rejected_low_confluence"] += 1
                log.debug(f"{symbol}: Low BTC-adjusted confluence {btc_adjusted_score:.1f}/10")
                return None
            
            # === STEP 8: Confluence-based deduplication ===
            if not self.signal_manager.should_generate_signal(symbol, btc_adjusted_score, alt_side):
                return None
            
            # === STEP 9: Determine entry parameters ===
            entry_type = self.determine_entry_type(
                market_structure, momentum, liquidity_zone, alt_side
            )
            
            entry_price = self._calculate_entry_price(
                alt_side, market_structure, liquidity_zone, current_price
            )
            
            stop_loss = self._calculate_stop_loss(
                alt_side, entry_price, market_structure, liquidity_zone
            )
            
            take_profit, expected_move_pct = self._calculate_take_profit(
                alt_side, entry_price, market_structure, btc_adjusted_score
            )
            
            # === STEP 10: Calculate risk/reward ===
            risk = abs(entry_price - stop_loss)
            reward = abs(take_profit - entry_price)
            
            if risk == 0:
                return None
            
            risk_reward = reward / risk
            risk_pct = risk / entry_price * 100
            reward_pct = reward / entry_price * 100
            
            if risk_reward < MIN_RISK_REWARD:
                log.debug(f"{symbol}: R:R too low {risk_reward:.1f}:1")
                return None
            
            # === STEP 11: Calculate entry confidence ===
            entry_confidence = self.calculate_entry_confidence(
                btc_adjusted_score, order_flow, momentum
            )
            
            # === STEP 12: Determine conditions met ===
            conditions_met = self._get_confluence_conditions(
                market_structure, order_flow, momentum, liquidity_zone, alt_side
            )
            
            # Add BTC alignment condition
            if btc_recommended_side == "BOTH":
                conditions_met.append("BTC_NEUTRAL_BOTH_SIDES")
            elif btc_recommended_side == alt_side:
                conditions_met.append("BTC_ALIGNED")
            
            # === STEP 13: Probability score ===
            probability_score = min(btc_adjusted_score / 10.0 * 1.2, 0.95)
            
            # === STEP 14: Create signal ID ===
            signal_id = hashlib.md5(
                f"{symbol}:{alt_side}:{btc_adjusted_score}:{time.time()}".encode()
            ).hexdigest()
            
            # === STEP 15: Create final signal ===
            signal = ConfluenceSetup(
                signal_id=signal_id,
                symbol=symbol,
                side=alt_side,
                
                market_structure=market_structure,
                order_flow=order_flow,
                momentum=momentum,
                liquidity_zone=liquidity_zone,
                
                entry_price=entry_price,
                entry_type=entry_type,
                entry_confidence=entry_confidence,
                
                stop_loss=stop_loss,
                take_profit=take_profit,
                risk_pct=risk_pct,
                reward_pct=reward_pct,
                risk_reward=risk_reward,
                
                confluence_score=btc_adjusted_score,
                confluence_details=confluence_details,
                conditions_met=conditions_met,
                
                expected_move_pct=expected_move_pct,
                probability_score=probability_score,
                
                timeframe_used="15M",
                signal_timestamp=time.time(),
                
                # BTC alignment info
                btc_alignment={
                    "btc_direction": self.btc_structure["direction"],
                    "btc_strength": self.btc_structure["trend_strength"],
                    "btc_regime": self.btc_structure["regime"],
                    "recommended_side": btc_recommended_side,
                    "original_score": confluence_score,
                    "btc_adjusted_score": btc_adjusted_score,
                    "alignment_multiplier": btc_adjusted_score / confluence_score if confluence_score > 0 else 1.0,
                    "btc_state": "NEUTRAL_BOTH_ALLOWED" if btc_recommended_side == "BOTH" else "ALIGNED"
                }
            )
            
            # === STEP 16: Register signal ===
            self.signal_manager.register_signal(signal)
            
            # === STEP 17: Update stats ===
            self.daily_stats["confluence_signals"] += 1
            if btc_adjusted_score >= 8.0:
                self.daily_stats["high_quality_signals"] += 1
            
            # Log appropriate message based on BTC state
            if btc_recommended_side == "BOTH":
                log.info(f"⚖️ BTC-NEUTRAL SIGNAL: {symbol} {alt_side} @ {entry_price:.4f}")
                log.info(f"   BTC Direction: NEUTRAL | Both sides allowed")
            else:
                log.info(f"🎯 BTC-ALIGNED SIGNAL: {symbol} {alt_side} @ {entry_price:.4f}")
                log.info(f"   BTC Direction: {self.btc_structure['direction']} | Regime: {self.btc_structure['regime']}")
            
            log.info(f"   Original Score: {confluence_score:.1f} | BTC-Adjusted: {btc_adjusted_score:.1f}")
            log.info(f"   Expected: {expected_move_pct:.1f}% | R:R: {risk_reward:.1f}:1")
            
            return signal
            
        except Exception as e:
            log.error(f"Confluence signal error for {symbol}: {e}")
            log.error(f"Traceback: {traceback.format_exc()}")
            return None
    
    def _determine_trade_side_original(self, structure: MarketStructure,
                                      momentum: MomentumSignal,
                                      liquidity: LiquidityZone,
                                      symbol: str) -> Optional[str]:
        """Original altcoin side determination (without BTC constraint)"""
        
        bullish_factors = []
        bearish_factors = []
        
        # 1. Market structure
        if structure.trend == "BULLISH":
            bullish_factors.append(("Trend BULLISH", 2))
        elif structure.trend == "BEARISH":
            bearish_factors.append(("Trend BEARISH", 2))
        
        # 2. HTF alignment
        if structure.higher_timeframe_aligned:
            if structure.trend == "BULLISH":
                bullish_factors.append(("HTF Aligned BULLISH", 1))
            elif structure.trend == "BEARISH":
                bearish_factors.append(("HTF Aligned BEARISH", 1))
        else:
            if structure.trend == "BULLISH":
                bearish_factors.append(("HTF Misaligned", -1))
            elif structure.trend == "BEARISH":
                bullish_factors.append(("HTF Misaligned", -1))
        
        # 3. Momentum
        if momentum.rsi_divergence == "BULLISH_HIDDEN":
            if structure.trend == "BULLISH":
                bullish_factors.append(("RSI Bullish Divergence (CONFIRMING)", 2))
            else:
                bullish_factors.append(("RSI Bullish Divergence (COUNTER)", 1))
        
        elif momentum.rsi_divergence == "BEARISH_HIDDEN":
            if structure.trend == "BEARISH":
                bearish_factors.append(("RSI Bearish Divergence (CONFIRMING)", 2))
            else:
                bearish_factors.append(("RSI Bearish Divergence (COUNTER)", 1))
        
        # 4. MACD signals
        if momentum.macd_signal in ["BULLISH_CROSS", "BULLISH_FLIP"]:
            bullish_factors.append((f"MACD {momentum.macd_signal}", 1))
        
        elif momentum.macd_signal in ["BEARISH_CROSS", "BEARISH_FLIP"]:
            bearish_factors.append((f"MACD {momentum.macd_signal}", 1))
        
        # 5. Liquidity zones
        if liquidity.zone_type in ["SWEEP_LOW", "EQ_LOW"]:
            bullish_factors.append((f"Liquidity {liquidity.zone_type}", 1))
        
        elif liquidity.zone_type in ["SWEEP_HIGH", "EQ_HIGH"]:
            bearish_factors.append((f"Liquidity {liquidity.zone_type}", 1))
        
        # Calculate scores
        bullish_score = sum(score for _, score in bullish_factors)
        bearish_score = sum(score for _, score in bearish_factors)
        
        log.debug(f"{symbol} Side Determination (Original):")
        log.debug(f"  Bullish factors ({bullish_score}): {bullish_factors}")
        log.debug(f"  Bearish factors ({bearish_score}): {bearish_factors}")
        
        if bullish_score >= 3 and bullish_score > bearish_score:
            return "LONG"
        
        elif bearish_score >= 3 and bearish_score > bullish_score:
            return "SHORT"
        
        return None
    
    def calculate_confluence_score(self, structure: MarketStructure,
                                  order_flow: OrderFlow, momentum: MomentumSignal,
                                  liquidity: LiquidityZone, side: str) -> Tuple[float, Dict]:
        """Calculate overall confluence score (0-10)"""
        scores = {}
        details = {}
        
        # 1. Market Structure Score (0-2.5)
        struct_score = structure.structure_score * 2.5
        scores["structure"] = struct_score
        details["structure"] = {
            "trend": str(structure.trend),
            "aligned": bool(structure.higher_timeframe_aligned),
            "raw_score": float(structure.structure_score)
        }
        
        # 2. Order Flow Score (0-3.0)
        flow_score = order_flow.flow_score * 3.0
        scores["order_flow"] = flow_score
        details["order_flow"] = {
            "volume_spike": bool(order_flow.volume_spike),
            "volume_ratio": float(order_flow.volume_ratio),
            "imbalance": float(order_flow.bid_ask_imbalance),
            "raw_score": float(order_flow.flow_score)
        }
        
        # 3. Momentum Score (0-2.5)
        mom_score = momentum.momentum_score * 2.5
        scores["momentum"] = mom_score
        details["momentum"] = {
            "rsi_divergence": str(momentum.rsi_divergence),
            "rsi_zone": str(momentum.rsi_zone),
            "macd_signal": str(momentum.macd_signal),
            "candle_pattern": str(momentum.candle_pattern),
            "raw_score": float(momentum.momentum_score)
        }
        
        # 4. Liquidity Score (0-2.0)
        if liquidity.zone_type != "NONE":
            liq_score = liquidity.strength * 2.0
            if liquidity.stop_hunt_potential:
                liq_score *= 1.2
        else:
            liq_score = 0.0
        
        scores["liquidity"] = liq_score
        details["liquidity"] = {
            "zone_type": str(liquidity.zone_type),
            "stop_hunt_potential": bool(liquidity.stop_hunt_potential),
            "strength": float(liquidity.strength)
        }
        
        # 5. Side-specific adjustments
        side_adjustment = 0.0
        if side == "LONG":
            if (structure.trend == "BULLISH" and
                momentum.rsi_divergence == "BULLISH_HIDDEN" and
                momentum.macd_signal in ["BULLISH_CROSS", "BULLISH_FLIP"]):
                side_adjustment = 0.5
        
        elif side == "SHORT":
            if (structure.higher_timeframe_aligned and
                momentum.rsi_divergence == "BEARISH_HIDDEN" and
                momentum.macd_signal in ["BEARISH_CROSS", "BEARISH_FLIP"]):
                side_adjustment = 0.5
        
        scores["side_adjustment"] = side_adjustment
        
        # Penalize misalignment
        if not structure.higher_timeframe_aligned and side == structure.trend:
            scores["alignment_penalty"] = -0.5
        elif not structure.higher_timeframe_aligned:
            scores["alignment_penalty"] = -1.0
        
        # Calculate total score (0-10)
        total_score = sum(scores.values())
        total_score = max(0.0, min(total_score, 10.0))
        
        return total_score, {
            "scores": scores,
            "details": details,
            "total": total_score
        }
    
    def determine_entry_type(self, structure: MarketStructure, momentum: MomentumSignal,
                           liquidity: LiquidityZone, side: str) -> str:
        """Determine entry type based on confluence"""
        
        if liquidity.recently_tested and "SWEEP" in liquidity.zone_type:
            return "BREAKOUT_RETEST" if side == "LONG" else "BREAKDOWN_RETEST"
        
        if structure.breaker_blocks:
            last_block = structure.breaker_blocks[-1]
            if side == "LONG" and last_block["type"] == "BULLISH_OB":
                return "ORDER_BLOCK_ENTRY"
            elif side == "SHORT" and last_block["type"] == "BEARISH_OB":
                return "ORDER_BLOCK_ENTRY"
        
        if momentum.candle_pattern in ["HAMMER", "BULLISH_ENGULFING", "INSIDE_BAR"]:
            return "SUPPORT_BOUNCE" if side == "LONG" else "RESISTANCE_BOUNCE"
        
        return "CONFLUENCE_ZONE_ENTRY"
    
    def calculate_entry_confidence(self, confluence_score: float,
                                  order_flow: OrderFlow, momentum: MomentumSignal) -> float:
        """Calculate entry confidence (0-1)"""
        base_confidence = confluence_score / 10.0
        
        if order_flow.volume_spike:
            base_confidence *= 1.2
        
        if momentum.momentum_score > 0.7:
            base_confidence *= 1.1
        
        if momentum.rsi_divergence != "NONE":
            base_confidence *= 1.15
        
        return min(base_confidence, 1.0)
    
    def _calculate_entry_price(self, side: str, structure: MarketStructure,
                              liquidity: LiquidityZone, current_price: float) -> float:
        """Calculate precise entry price"""
        
        if liquidity.zone_type != "NONE" and liquidity.price_level > 0:
            zone_price = liquidity.price_level
            
            if side == "LONG":
                return zone_price * 1.001
            else:
                return zone_price * 0.999
        
        if side == "LONG" and structure.key_support > 0:
            return structure.key_support * 1.001
        elif side == "SHORT" and structure.key_resistance > 0:
            return structure.key_resistance * 0.999
        
        return current_price
    
    def _calculate_stop_loss(self, side: str, entry_price: float,
                            structure: MarketStructure, liquidity: LiquidityZone) -> float:
        """Calculate stop loss based on structure"""
        
        if side == "LONG":
            if structure.swing_lows:
                recent_low = min(structure.swing_lows)
                stop_loss = min(recent_low * 0.995, entry_price * 0.985)
            else:
                stop_loss = entry_price * 0.985
            
            max_sl = entry_price * (1 - MAX_STOP_LOSS / 100)
            stop_loss = min(stop_loss, max_sl)
        
        else:
            if structure.swing_highs:
                recent_high = max(structure.swing_highs)
                stop_loss = max(recent_high * 1.005, entry_price * 1.015)
            else:
                stop_loss = entry_price * 1.015
            
            min_sl = entry_price * (1 + MAX_STOP_LOSS / 100)
            stop_loss = max(stop_loss, min_sl)
        
        return stop_loss
    
    def _calculate_take_profit(self, side: str, entry_price: float,
                              structure: MarketStructure, confluence_score: float) -> Tuple[float, float]:
        """Calculate take profit for 3-5% move"""
        
        base_target_pct = TARGET_PROFIT_RANGE[0] + (
            (confluence_score - MIN_CONFLUENCE_SCORE) /
            (10 - MIN_CONFLUENCE_SCORE) *
            (TARGET_PROFIT_RANGE[1] - TARGET_PROFIT_RANGE[0])
        )
        
        target_pct = max(TARGET_PROFIT_RANGE[0],
                        min(base_target_pct, TARGET_PROFIT_RANGE[1]))
        
        if structure.key_resistance > 0 and side == "LONG":
            resistance_pct = (structure.key_resistance - entry_price) / entry_price * 100
            if 2.0 <= resistance_pct <= 8.0:
                target_pct = min(target_pct, resistance_pct * 0.8)
        
        elif structure.key_support > 0 and side == "SHORT":
            support_pct = (entry_price - structure.key_support) / entry_price * 100
            if 2.0 <= support_pct <= 8.0:
                target_pct = min(target_pct, support_pct * 0.8)
        
        if side == "LONG":
            take_profit = entry_price * (1 + target_pct / 100)
        else:
            take_profit = entry_price * (1 - target_pct / 100)
        
        return take_profit, target_pct
    
    def _get_confluence_conditions(self, structure: MarketStructure,
                                  order_flow: OrderFlow, momentum: MomentumSignal,
                                  liquidity: LiquidityZone, side: str) -> List[str]:
        """Get list of confluence conditions met"""
        conditions = []
        
        conditions.append(f"TREND_{structure.trend}")
        if structure.higher_timeframe_aligned:
            conditions.append("HTF_ALIGNED")
        else:
            conditions.append("HTF_MISALIGNED")
        
        if order_flow.volume_spike:
            conditions.append("VOLUME_SPIKE")
        if order_flow.bid_ask_imbalance > 0.3:
            conditions.append("BID_IMBALANCE")
        elif order_flow.bid_ask_imbalance < -0.3:
            conditions.append("ASK_IMBALANCE")
        
        if momentum.rsi_divergence != "NONE":
            conditions.append(f"RSI_{momentum.rsi_divergence}")
        if momentum.macd_signal != "NONE":
            conditions.append(f"MACD_{momentum.macd_signal}")
        if momentum.candle_pattern != "NONE":
            conditions.append(f"CANDLE_{momentum.candle_pattern}")
        
        if liquidity.zone_type != "NONE":
            conditions.append(f"LIQ_{liquidity.zone_type}")
        if liquidity.stop_hunt_potential:
            conditions.append("STOP_HUNT_POTENTIAL")
        
        if side == "LONG":
            conditions.append("ENTRY_LONG")
        else:
            conditions.append("ENTRY_SHORT")
        
        return conditions
    
    def get_daily_stats(self) -> Dict:
        """Get daily statistics"""
        return self.daily_stats
    
    def cleanup_old_signals(self):
        """Clean up old signals"""
        self.signal_manager.cleanup_old_signals()

# ================ MAIN SCANNER SYSTEM ================
class BTCConfluenceMoveScanner:
    """Main BTC-centric scanner for 3-5% confluence moves"""
    
    def __init__(self):
        self.scanner = BTCConfluenceScanner()
        self.exchange = None
        self.db = None
        self.scan_cycle = 0
        self.data_cache = {}
        self.cache_ttl = 60
    
    async def initialize(self):
        """Initialize the scanner"""
        log.info("=" * 70)
        log.info("🎯 BTC-CENTRIC CONFLUENCE SCANNER v6.0 (MODIFIED)")
        log.info("BITCOIN IS PRIMARY - When BTC has direction, follow it")
        log.info("When BTC is neutral, both LONG and SHORT allowed")
        log.info("=" * 70)
        log.info("EXCHANGE: OKX")
        log.info("STRATEGY: BTC structure + Alt confluence")
        log.info("TARGET: 3-5% BTC-aligned moves")
        log.info("PHILOSOPHY: Trade with BTC trend, both sides when neutral")
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
            CREATE TABLE IF NOT EXISTS btc_confluence_signals (
                id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                entry_price REAL NOT NULL,
                stop_loss REAL NOT NULL,
                take_profit REAL NOT NULL,
                
                confluence_score REAL NOT NULL,
                btc_adjusted_score REAL NOT NULL,
                confluence_details TEXT,
                conditions_met TEXT,
                
                btc_direction TEXT,
                btc_strength REAL,
                btc_regime TEXT,
                btc_alignment TEXT,
                
                expected_move REAL NOT NULL,
                probability_score REAL NOT NULL,
                entry_confidence REAL NOT NULL,
                entry_type TEXT NOT NULL,
                
                risk_pct REAL NOT NULL,
                reward_pct REAL NOT NULL,
                risk_reward REAL NOT NULL,
                
                market_structure TEXT,
                order_flow TEXT,
                momentum TEXT,
                liquidity_zone TEXT,
                
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
            
            await self.db.execute("""
            CREATE TABLE IF NOT EXISTS btc_confluence_performance (
                date DATE PRIMARY KEY,
                total_signals INTEGER,
                btc_aligned_signals INTEGER,
                btc_neutral_signals INTEGER,
                avg_btc_strength REAL,
                avg_confluence_score REAL,
                avg_btc_adjusted_score REAL,
                win_rate REAL,
                avg_pnl REAL,
                total_pnl REAL
            )
            """)
            
            await self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_btc_signals_symbol ON btc_confluence_signals(symbol)
            """)
            await self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_btc_signals_btc_dir ON btc_confluence_signals(btc_direction)
            """)
            
            await self.db.commit()
            log.info("✅ BTC-centric database initialized")
            
        except Exception as e:
            log.error(f"Database error: {e}")
            raise
    
    async def _init_exchange(self):
        """Initialize OKX exchange connection"""
        try:
            self.exchange = ccxt.okx({
                "enableRateLimit": True,
                "options": {
                    "defaultType": "spot",
                    "fetchMarkets": "spot",
                    "adjustForTimeDifference": True,
                },
                "timeout": 30000,
                "rateLimit": 20,
            })
            
            markets = await self.exchange.fetch_markets(params={'type': 'spot'})
            usdt_pairs = [m['symbol'] for m in markets if m['symbol'].endswith('/USDT')]
            
            log.info(f"✅ OKX exchange connected. Found {len(usdt_pairs)} USDT pairs")
            
        except Exception as e:
            log.error(f"OKX exchange error: {e}")
            try:
                self.exchange = ccxt.bybit({
                    "enableRateLimit": True,
                    "options": {"defaultType": "spot"},
                    "timeout": 20000,
                })
                
                ticker = await self.exchange.fetch_ticker("BTC/USDT")
                log.info(f"✅ Bybit fallback connected. BTC: ${ticker['last']:.2f}")
                
            except Exception as fallback_error:
                log.error(f"❌ All exchanges failed: {fallback_error}")
                raise
    
    async def _send_startup_message(self):
        """Send startup message to Telegram"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            log.warning("⚠️ Telegram credentials not set")
            return
        
        try:
            message = """🎯 <b>BTC-CENTRIC CONFLUENCE SCANNER v6.0 (MODIFIED) - ONLINE</b>

<b>₿ PRIMARY:</b> Bitcoin structure determines trades
<b>🎯 MODIFIED RULES:</b>
   • When BTC has clear direction (≥4/10 strength): Alts MUST follow
   • When BTC is NEUTRAL: Both LONG and SHORT allowed
<b>📊 LOGIC:</b> Trade with BTC trend, both sides when neutral
<b>⚡ SIGNALS:</b> Confluence + BTC context
<b>🛡️ SAFETY:</b> No counter-BTC trades when BTC has direction

Scanner actively hunting for BTC-context setups.
BTC direction informs, doesn't always dictate.

#BTCContext #ConfluenceTrading #OKX #ModifiedRules #Ready"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML"
                })
                
            log.info("✅ BTC-centric startup message sent")
                
        except Exception as e:
            log.error(f"Telegram error: {e}")
    
    async def _fetch_single_timeframe(self, symbol: str, timeframe: str,
                                     limit: int, tf_name: str) -> pd.DataFrame:
        """Fetch single timeframe data"""
        try:
            cache_key = f"{symbol}_{tf_name}"
            if cache_key in self.data_cache:
                data, timestamp = self.data_cache[cache_key]
                if time.time() - timestamp < self.cache_ttl:
                    return data
            
            params = {'type': 'spot'}
            
            ohlcv = await self.exchange.fetch_ohlcv(
                symbol,
                timeframe=timeframe,
                limit=limit,
                params=params
            )
            
            if ohlcv and len(ohlcv) >= 15:
                df = pd.DataFrame(
                    ohlcv,
                    columns=["timestamp", "open", "high", "low", "close", "volume"]
                )
                
                for col in ["open", "high", "low", "close", "volume"]:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                
                df = df.dropna()
                
                if len(df) >= 15:
                    self.data_cache[cache_key] = (df, time.time())
                    return df
            
            return pd.DataFrame()
            
        except Exception as e:
            log.debug(f"OHLCV error {symbol} {tf_name}: {str(e)[:50]}")
            return pd.DataFrame()
    
    async def fetch_timeframe_data(self, symbol: str) -> Dict[str, pd.DataFrame]:
        """Batch fetch OHLCV data for multiple timeframes"""
        data = {}
        tasks = []
        
        limit_map = {
            "DAILY": 100,
            "4H": 120,
            "1H": 168,
            "15M": 96,
            "5M": 72
        }
        
        for tf_name, tf in TIMEFRAMES.items():
            limit = limit_map.get(tf_name, 50)
            tasks.append(self._fetch_single_timeframe(symbol, tf, limit, tf_name))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for tf_name, result in zip(TIMEFRAMES.keys(), results):
            if isinstance(result, pd.DataFrame) and not result.empty:
                data[tf_name] = result
        
        return data
    
    async def get_active_pairs(self) -> List[Tuple[str, float]]:
        """Get active trading pairs from OKX"""
        try:
            markets = await self.exchange.fetch_markets(params={'type': 'spot'})
            
            active_pairs = []
            
            for market in markets:
                symbol = market['symbol']
                
                if symbol.endswith('/USDT'):
                    try:
                        ticker = await self.exchange.fetch_ticker(symbol)
                        volume = ticker.get('quoteVolume', 0)
                        
                        if volume >= MIN_VOLUME_USD:
                            price = ticker.get('last', 0)
                            if price > 0.01:
                                active_pairs.append((symbol, volume))
                    except Exception as e:
                        continue
            
            active_pairs.sort(key=lambda x: x[1], reverse=True)
            selected_pairs = active_pairs[:TOP_N_VOLUME]
            
            log.info(f"📊 Selected {len(selected_pairs)} pairs from OKX")
            return selected_pairs
            
        except Exception as e:
            log.error(f"Error getting OKX pairs: {e}")
            return []
    
    def make_json_serializable(self, obj):
        """Helper function to ensure JSON serializable data"""
        if isinstance(obj, (bool, np.bool_)):
            return bool(obj)
        elif isinstance(obj, (int, np.integer)):
            return int(obj)
        elif isinstance(obj, (float, np.floating)):
            return float(obj)
        elif isinstance(obj, str):
            return str(obj)
        elif isinstance(obj, dict):
            return {k: self.make_json_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.make_json_serializable(item) for item in obj]
        elif hasattr(obj, '__dict__'):
            return str(obj)
        elif obj is None:
            return None
        else:
            try:
                return float(obj)
            except:
                return str(obj)
    
    async def save_signal(self, signal: ConfluenceSetup) -> bool:
        """Save signal to database"""
        try:
            # Prepare market structure data
            market_structure_data = {
                "trend": str(signal.market_structure.trend),
                "aligned": bool(signal.market_structure.higher_timeframe_aligned),
                "key_support": float(signal.market_structure.key_support),
                "key_resistance": float(signal.market_structure.key_resistance),
                "score": float(signal.market_structure.structure_score)
            }
            
            # Prepare order flow data
            order_flow_data = {
                "volume_spike": bool(signal.order_flow.volume_spike),
                "volume_ratio": float(signal.order_flow.volume_ratio),
                "imbalance": float(signal.order_flow.bid_ask_imbalance),
                "score": float(signal.order_flow.flow_score)
            }
            
            # Prepare momentum data
            momentum_data = {
                "rsi_divergence": str(signal.momentum.rsi_divergence),
                "rsi_value": float(signal.momentum.rsi_value),
                "macd_signal": str(signal.momentum.macd_signal),
                "candle_pattern": str(signal.momentum.candle_pattern),
                "score": float(signal.momentum.momentum_score)
            }
            
            # Prepare liquidity zone data
            liquidity_data = {
                "zone_type": str(signal.liquidity_zone.zone_type),
                "price_level": float(signal.liquidity_zone.price_level),
                "stop_hunt_potential": bool(signal.liquidity_zone.stop_hunt_potential),
                "strength": float(signal.liquidity_zone.strength)
            }
            
            # Prepare conditions met
            conditions_met = [str(condition) for condition in signal.conditions_met]
            
            # Make confluence details serializable
            confluence_details = self.make_json_serializable(signal.confluence_details)
            
            # BTC alignment data
            btc_alignment = signal.btc_alignment or {}
            
            # Get original score from details
            original_score = signal.confluence_details.get("total", signal.confluence_score)
            btc_adjusted_score = signal.confluence_score
            
            # Insert signal
            await self.db.execute("""
                INSERT INTO btc_confluence_signals (
                    id, symbol, side, entry_price, stop_loss, take_profit,
                    confluence_score, btc_adjusted_score, confluence_details, conditions_met,
                    btc_direction, btc_strength, btc_regime, btc_alignment,
                    expected_move, probability_score, entry_confidence, entry_type,
                    risk_pct, reward_pct, risk_reward,
                    market_structure, order_flow, momentum, liquidity_zone
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                str(signal.signal_id),
                str(signal.symbol),
                str(signal.side),
                float(signal.entry_price),
                float(signal.stop_loss),
                float(signal.take_profit),
                float(original_score),
                float(btc_adjusted_score),
                json.dumps(confluence_details),
                json.dumps(conditions_met),
                str(btc_alignment.get("btc_direction", "UNKNOWN")),
                float(btc_alignment.get("btc_strength", 0.0)),
                str(btc_alignment.get("btc_regime", "UNKNOWN")),
                json.dumps(btc_alignment),
                float(signal.expected_move_pct),
                float(signal.probability_score),
                float(signal.entry_confidence),
                str(signal.entry_type),
                float(signal.risk_pct),
                float(signal.reward_pct),
                float(signal.risk_reward),
                json.dumps(market_structure_data),
                json.dumps(order_flow_data),
                json.dumps(momentum_data),
                json.dumps(liquidity_data)
            ))
            
            await self.db.commit()
            log.info(f"✅ BTC-confluence signal saved: {signal.symbol}")
            return True
            
        except Exception as e:
            log.error(f"Error saving signal: {e}")
            return False
    
    async def format_confluence_signal(self, signal: ConfluenceSetup) -> str:
        """Format BTC-centric confluence signal"""
        
        side_emoji = "🟢" if signal.side == "LONG" else "🔴"
        clean_symbol = signal.symbol.replace('/', '')
        
        # BTC alignment info
        btc_info = signal.btc_alignment or {}
        btc_direction = btc_info.get("btc_direction", "UNKNOWN")
        btc_regime = btc_info.get("btc_regime", "UNKNOWN")
        btc_strength = btc_info.get("btc_strength", 0.0)
        btc_recommended = btc_info.get("recommended_side", "UNKNOWN")
        original_score = btc_info.get("original_score", signal.confluence_score)
        btc_adjusted_score = btc_info.get("btc_adjusted_score", signal.confluence_score)
        btc_state = btc_info.get("btc_state", "UNKNOWN")
        
        # Confluence score color coding
        if btc_adjusted_score >= 8.5:
            score_emoji = "🔥🔥"
            score_text = "EXCEPTIONAL"
        elif btc_adjusted_score >= 7.5:
            score_emoji = "🔥"
            score_text = "HIGH"
        elif btc_adjusted_score >= 6.0:
            score_emoji = "✅"
            score_text = "GOOD"
        else:
            score_emoji = "⚠️"
            score_text = "FAIR"
        
        # BTC alignment status
        if btc_state == "NEUTRAL_BOTH_ALLOWED":
            btc_emoji = "⚖️"  # Scale emoji for neutral
            alignment_text = "BTC NEUTRAL - BOTH SIDES ALLOWED"
        elif btc_direction == signal.side:
            btc_emoji = "✅"
            alignment_text = "PERFECT BTC ALIGNMENT"
        else:
            btc_emoji = "⚠️"
            alignment_text = "COUNTER BTC (should be rejected)"
        
        # Structure summary
        structure_details = [
            f"📊 Trend: {signal.market_structure.trend}",
            f"🎯 Alignment: {'YES' if signal.market_structure.higher_timeframe_aligned else 'NO'}",
            f"📍 Support: {signal.market_structure.key_support:.4f}",
            f"📍 Resistance: {signal.market_structure.key_resistance:.4f}"
        ]
        
        # BTC context
        btc_details = [
            f"₿ BTC Direction: <b>{btc_direction}</b>",
            f"🏋️ BTC Strength: {btc_strength:.1f}/10",
            f"📈 BTC Regime: {btc_regime}",
            f"⚖️ Alignment: {btc_emoji} {alignment_text}"
        ]
        
        # Order flow summary
        flow_details = [
            f"📈 Volume: {'SPIKE' if signal.order_flow.volume_spike else 'Normal'}",
            f"⚖️ Imbalance: {signal.order_flow.bid_ask_imbalance:+.2f}",
            f"💧 Flow Score: {signal.order_flow.flow_score:.2f}/1.0"
        ]
        
        # Momentum summary
        momentum_details = [
            f"📉 RSI: {signal.momentum.rsi_value:.1f} ({signal.momentum.rsi_zone})",
            f"🔀 Divergence: {signal.momentum.rsi_divergence.replace('_', ' ') if signal.momentum.rsi_divergence != 'NONE' else 'None'}",
            f"📊 MACD: {signal.momentum.macd_signal.replace('_', ' ') if signal.momentum.macd_signal != 'NONE' else 'None'}"
        ]
        
        # Entry details
        entry_details = [
            f"🎪 Type: {signal.entry_type.replace('_', ' ')}",
            f"🎯 Confidence: {signal.entry_confidence:.0%}"
        ]
        
        # Risk management
        risk_details = [
            f"🛡️ Risk: {signal.risk_pct:.2f}%",
            f"💰 Reward: {signal.reward_pct:.2f}%",
            f"⚖️ R:R: {signal.risk_reward:.1f}:1",
            f"🎯 Target: {signal.expected_move_pct:.1f}%"
        ]
        
        # Score details
        score_details = [
            f"📊 Original: {original_score:.1f}/10",
            f"₿ BTC-Adjusted: {btc_adjusted_score:.1f}/10",
            f"📈 Multiplier: {btc_adjusted_score/original_score:.2f}x" if original_score > 0 else "📈 Multiplier: N/A"
        ]
        
        # Build the message
        message = f"""{side_emoji} <b>BTC-CONTEXT CONFLUENCE SIGNAL - {signal.side}</b>

{btc_emoji} <b>₿ BITCOIN CONTEXT:</b>
{chr(10).join(btc_details)}

<b>{score_emoji} CONFLUENCE: {score_text} ({btc_adjusted_score:.1f}/10)</b>
<b>📊 {signal.symbol}</b> | <b>🎪 {signal.entry_type.replace('_', ' ')}</b>

<b>🔢 SCORE DETAILS:</b>
{chr(10).join(score_details)}

<b>🏗️ STRUCTURE:</b>
{chr(10).join(structure_details)}

<b>💧 ORDER FLOW:</b>
{chr(10).join(flow_details)}

<b>📈 MOMENTUM:</b>
{chr(10).join(momentum_details)}

<b>🎯 ENTRY:</b>
• Price: <b>{signal.entry_price:.6f}</b>
• SL: {signal.stop_loss:.6f}
• TP: {signal.take_profit:.6f}
{chr(10).join(entry_details)}

<b>⚖️ RISK/REWARD:</b>
{chr(10).join(risk_details)}

<b>📋 CONDITIONS:</b>
• Met: {len(signal.conditions_met)} conditions
• BTC State: {'NEUTRAL (BOTH SIDES)' if 'BTC_NEUTRAL_BOTH_SIDES' in signal.conditions_met else 'ALIGNED'}
• Probability: {signal.probability_score:.0%}

#BTC{btc_direction} #{clean_symbol} #{signal.side}
#Expected{signal.expected_move_pct:.0f}Percent #OKX"""
        
        return message
    
    async def send_telegram_alert(self, signal: ConfluenceSetup):
        """Send Telegram alert"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            log.warning(f"⚠️ Telegram credentials missing. Skipping alert for {signal.symbol}")
            return
        
        try:
            message = await self.format_confluence_signal(signal)
            
            if len(message) > 3800:
                # Compact format
                side_emoji = "🟢" if signal.side == "LONG" else "🔴"
                btc_info = signal.btc_alignment or {}
                btc_direction = btc_info.get("btc_direction", "UNKNOWN")
                btc_state = btc_info.get("btc_state", "UNKNOWN")
                
                message = f"""{side_emoji} <b>BTC-{btc_direction} | {signal.side} {signal.symbol}</b>

<b>CONFLUENCE: {signal.confluence_score:.1f}/10</b>
• Entry: {signal.entry_price:.6f}
• Target: {signal.expected_move_pct:.1f}%
• R:R: {signal.risk_reward:.1f}:1
• BTC Context: {'NEUTRAL (BOTH)' if btc_state == 'NEUTRAL_BOTH_ALLOWED' else 'ALIGNED'}

<b>KEY:</b>
• BTC: {btc_direction} ({btc_info.get('btc_strength', 0):.1f}/10)
• Trend: {signal.market_structure.trend}
• Volume: {'Spike' if signal.order_flow.volume_spike else 'Normal'}
• Momentum: {signal.momentum.rsi_divergence.split('_')[0] if signal.momentum.rsi_divergence != 'NONE' else 'Neutral'}

SL: {signal.stop_loss:.6f} | TP: {signal.take_profit:.6f}

#BTC{btc_direction} #{signal.symbol.replace('/', '')} #OKX"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            
            payload = {
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True
            }
            
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.post(url, json=payload)
                
                if response.status_code == 400:
                    plain_message = message.replace('<b>', '').replace('</b>', '')
                    payload = {
                        "chat_id": TELEGRAM_CHAT_ID,
                        "text": plain_message,
                        "disable_web_page_preview": True
                    }
                    await client.post(url, json=payload)
            
            log.info(f"📤 BTC-confluence alert sent: {signal.symbol}")
            
        except Exception as e:
            log.error(f"Telegram error for {signal.symbol}: {e}")
    
    async def send_triggered_position_alert(self, symbol: str, side: str, trigger_price: float):
        """Send Telegram alert when position triggers"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return
        
        try:
            side_emoji = "🟢" if side == "LONG" else "🔴"
            clean_symbol = symbol.replace('/', '').replace('-', '').replace('.', '')
            
            message = f"""✅ <b>BTC-CONTEXT POSITION TRIGGERED - {side} {side_emoji}</b>

<b>Symbol:</b> {symbol}
<b>Trigger Price:</b> {trigger_price:.6f}
<b>Status:</b> Position is now active.

Stop Loss and Take Profit are now active.
Position auto-closes at SL/TP.

#{clean_symbol} #{side} #Triggered #OKX"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            
            payload = {
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True
            }
            
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.post(url, json=payload)
                
                if response.status_code == 400:
                    plain_message = message.replace('<b>', '').replace('</b>', '')
                    payload = {
                        "chat_id": TELEGRAM_CHAT_ID,
                        "text": plain_message,
                        "disable_web_page_preview": True
                    }
                    await client.post(url, json=payload)
            
            log.info(f"📤 Triggered position alert sent: {symbol}")
            
        except Exception as e:
            log.error(f"Triggered position alert error: {e}")
    
    async def send_closed_position_alert(self, symbol: str, side: str, entry_price: float,
                                        close_price: float, pnl_percent: float, close_reason: str):
        """Send Telegram alert when position closes"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return
        
        try:
            side_emoji = "🟢" if side == "LONG" else "🔴"
            
            if pnl_percent > 0:
                pnl_emoji = "💰"
                pnl_color = "#00FF00"
                result_text = "PROFIT"
            else:
                pnl_emoji = "💸"
                pnl_color = "#FF0000"
                result_text = "LOSS"
            
            clean_symbol = symbol.replace('/', '').replace('-', '').replace('.', '')
            
            message = f"""{pnl_emoji} <b>BTC-CONTEXT POSITION CLOSED - {side} {side_emoji}</b>

<b>Symbol:</b> {symbol}
<b>Side:</b> {side}
<b>Entry Price:</b> {entry_price:.6f}
<b>Close Price:</b> {close_price:.6f}
<b>PNL:</b> <font color='{pnl_color}'>{pnl_percent:+.2f}%</font>
<b>Reason:</b> {close_reason}

#{clean_symbol} #{result_text} #{side} #{close_reason} #OKX"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            
            payload = {
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True
            }
            
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.post(url, json=payload)
                
                if response.status_code == 400:
                    plain_message = message.replace('<b>', '').replace('</b>', '').replace('<font color=\'', '').replace('\'>', '').replace('</font>', '')
                    payload = {
                        "chat_id": TELEGRAM_CHAT_ID,
                        "text": plain_message,
                        "disable_web_page_preview": True
                    }
                    await client.post(url, json=payload)
            
            log.info(f"📤 Closed position alert sent: {symbol} {pnl_percent:+.2f}% ({close_reason})")
            
        except Exception as e:
            log.error(f"Closed position alert error: {e}")
    
    async def monitor_positions(self):
        """Monitor and close positions"""
        log.info("👀 Starting BTC-context position monitoring...")
        
        while True:
            try:
                async with self.db.execute("""
                    SELECT id, symbol, side, entry_price, stop_loss, take_profit, status
                    FROM btc_confluence_signals
                    WHERE status IN ('PENDING', 'TRIGGERED')
                """) as cursor:
                    positions = await cursor.fetchall()
                
                if positions:
                    log.debug(f"📊 Monitoring {len(positions)} BTC-context positions")
                
                for pos_id, symbol, side, entry, sl, tp, status in positions:
                    try:
                        ticker = await self.exchange.fetch_ticker(symbol)
                        current_price = ticker['last']
                        
                        if status == 'PENDING':
                            if abs(current_price - entry) / entry <= 0.01:
                                await self.db.execute("""
                                    UPDATE btc_confluence_signals SET
                                        status = 'TRIGGERED',
                                        triggered_at = CURRENT_TIMESTAMP,
                                        trigger_price = ?
                                    WHERE id = ?
                                """, (current_price, pos_id))
                                
                                await self.db.commit()
                                self.scanner.signal_manager.update_signal_status(pos_id, "TRIGGERED")
                                
                                await self.send_triggered_position_alert(symbol, side, current_price)
                                
                                log.info(f"✅ BTC-context position triggered: {symbol} {side} @ {current_price:.4f}")
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
                                UPDATE btc_confluence_signals SET
                                    status = 'CLOSED',
                                    closed_at = CURRENT_TIMESTAMP,
                                    close_price = ?,
                                    pnl_percent = ?,
                                    close_reason = ?
                                WHERE id = ?
                            """, (current_price, pnl_percent, close_reason, pos_id))
                            
                            await self.db.commit()
                            
                            result = "LOSS" if pnl_percent < 0 else "WIN"
                            self.scanner.signal_manager.update_signal_status(pos_id, "CLOSED", result)
                            
                            await self.send_closed_position_alert(
                                symbol, side, entry, current_price, pnl_percent, close_reason
                            )
                            
                            log.info(f"📤 BTC-context position closed: {symbol} {side} {pnl_percent:+.2f}% ({close_reason})")
                    
                    except Exception as e:
                        log.error(f"Monitor error for {symbol}: {e}")
                        continue
                
                self.scanner.cleanup_old_signals()
                
                current_time = time.time()
                to_remove = [k for k, (_, t) in self.data_cache.items() if current_time - t > self.cache_ttl]
                for k in to_remove:
                    del self.data_cache[k]
                
                await asyncio.sleep(5)
                
            except Exception as e:
                log.error(f"Monitoring loop error: {e}")
                await asyncio.sleep(10)
    
    async def process_single_pair(self, symbol: str, volume: float):
        """Process a single pair for BTC-centric confluence signals"""
        try:
            multi_tf_data = await self.fetch_timeframe_data(symbol)
            
            required_tfs = ["DAILY", "4H", "1H", "15M", "5M"]
            has_all_data = True
            
            for tf in required_tfs:
                df = multi_tf_data.get(tf)
                if df is None or df.empty or len(df) < 30:
                    has_all_data = False
                    break
            
            if not has_all_data:
                return None
            
            signal = await self.scanner.generate_confluence_signal(multi_tf_data, symbol, self.exchange)
            
            if signal:
                saved = await self.save_signal(signal)
                
                if saved:
                    await self.send_telegram_alert(signal)
                    return signal
            
            return None
            
        except Exception as e:
            log.debug(f"Pair error {symbol}: {str(e)[:50]}")
            return None
    
    async def btc_confluence_scanning(self):
        """Main BTC-centric scanning loop"""
        log.info("🚀 Starting BTC-context confluence scanning...")
        
        while True:
            try:
                self.scan_cycle += 1
                start_time = time.time()
                
                log.info(f"🔄 BTC-context scan #{self.scan_cycle}")
                
                pairs = await self.get_active_pairs()
                
                if not pairs:
                    log.warning("No active pairs found")
                    await asyncio.sleep(SCAN_INTERVAL)
                    continue
                
                log.info(f"Analyzing {len(pairs)} pairs for BTC-context confluence")
                
                signals_found = 0
                
                batch_size = 10
                for i in range(0, len(pairs), batch_size):
                    batch = pairs[i:i+batch_size]
                    batch_tasks = []
                    
                    for symbol, volume in batch:
                        batch_tasks.append(self.process_single_pair(symbol, volume))
                    
                    batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                    
                    for result in batch_results:
                        if isinstance(result, ConfluenceSetup):
                            signals_found += 1
                    
                    await asyncio.sleep(0.5)
                
                stats = self.scanner.get_daily_stats()
                active_count = len(self.scanner.signal_manager.active_signals)
                
                log.info(f"📊 BTC-CONTEXT STATS:")
                log.info(f"   Found: {signals_found} | Active: {active_count}")
                log.info(f"   Total signals: {stats['confluence_signals']}")
                log.info(f"   BTC-aligned: {stats['btc_aligned_signals']}")
                log.info(f"   BTC-neutral (both sides): {stats['btc_neutral_signals']}")
                log.info(f"   Rejected (counter-BTC): {stats['rejected_counter_btc']}")
                log.info(f"   Rejected (BTC neutral/weak): {stats['rejected_btc_neutral']}")
                log.info(f"   BTC direction: {stats['btc_direction']}")
                
                scan_duration = time.time() - start_time
                log.info(f"Scan #{self.scan_cycle}: {signals_found} BTC-context signals in {scan_duration:.2f}s")
                
                wait_time = max(1.0, SCAN_INTERVAL - scan_duration)
                await asyncio.sleep(wait_time)
                
            except Exception as e:
                log.error(f"BTC scanning loop error: {e}")
                await asyncio.sleep(10)
    
    async def run(self):
        """Run the BTC-centric scanner"""
        try:
            await self.initialize()
            
            await asyncio.gather(
                self.btc_confluence_scanning(),
                self.monitor_positions()
            )
            
        except KeyboardInterrupt:
            log.info("BTC-context scanner stopped by user")
            await self.send_final_stats()
            
        except Exception as e:
            log.error(f"BTC scanner crashed: {e}")
            
        finally:
            await self.cleanup()
    
    async def send_final_stats(self):
        """Send final statistics"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return
        
        try:
            stats = self.scanner.get_daily_stats()
            active_count = len(self.scanner.signal_manager.active_signals)
            
            message = f"""🛑 <b>BTC-CONTEXT CONFLUENCE SCANNER v6.0 STOPPED</b>

<b>📊 FINAL BTC-CONTEXT STATS:</b>
• Exchange: OKX
• Total scans: {stats['scans']}
• Total pairs analyzed: {stats['pairs_analyzed']}
• BTC-aligned signals: {stats['btc_aligned_signals']}
• BTC-neutral signals (both sides): {stats['btc_neutral_signals']}
• High quality (8+): {stats['high_quality_signals']}

<b>🚫 REJECTIONS:</b>
• Counter-BTC trades: {stats['rejected_counter_btc']}
• BTC neutral/weak: {stats['rejected_btc_neutral']}
• Low confluence: {stats['rejected_low_confluence']}
• No alignment: {stats['rejected_no_alignment']}

<b>₿ BTC PERFORMANCE:</b>
• Final BTC direction: {stats['btc_direction']}
• Active BTC-context signals: {active_count}

<b>🎯 MODIFIED STRATEGY:</b>
• Traded with BTC trend when clear
• Allowed both sides when BTC neutral
• Higher opportunity capture

#BTCContext #FinalStats #OKX #ModifiedRules"""
            
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

# ================ HTTP SERVER ================
async def start_confluence_server(scanner, port=8000):
    """Start HTTP server for monitoring"""
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
                active_count = len(scanner.scanner.signal_manager.active_signals)
                
                response = json.dumps({
                    "status": "running",
                    "scanner": "BTC-Context Confluence Scanner v6.0 (Modified)",
                    "exchange": "OKX",
                    "primary_structure": "Bitcoin (context, not dictator)",
                    "strategy": "Trade with BTC trend, both sides when neutral",
                    "btc_min_trend": BTC_MIN_TREND_STRENGTH,
                    "scan_cycle": scanner.scan_cycle,
                    "active_signals": active_count,
                    "btc_direction": stats["btc_direction"],
                    "btc_aligned_signals": stats["btc_aligned_signals"],
                    "btc_neutral_signals": stats["btc_neutral_signals"],
                    "rejected_counter_btc": stats["rejected_counter_btc"],
                    "daily_stats": stats
                }, indent=2)
            
            elif path == '/btc':
                if scanner.scanner.btc_structure:
                    response = json.dumps(scanner.scanner.btc_structure, indent=2)
                else:
                    response = json.dumps({"error": "BTC structure not available yet"})
            
            elif path == '/signals':
                if scanner.db:
                    scanner.db.row_factory = aiosqlite.Row
                    async with scanner.db.execute("""
                        SELECT symbol, side, entry_price, btc_adjusted_score, 
                               btc_direction, btc_strength, expected_move, risk_reward,
                               entry_type, status, created_at, close_reason, pnl_percent
                        FROM btc_confluence_signals
                        ORDER BY created_at DESC
                        LIMIT 20
                    """) as cursor:
                        rows = await cursor.fetchall()
                        signals = [dict(row) for row in rows]
                    
                    response = json.dumps({
                        "signals": signals,
                        "count": len(signals),
                        "btc_centered": True
                    }, indent=2)
                else:
                    response = json.dumps({"error": "Database not available"})
            
            elif path == '/strategy':
                response = json.dumps({
                    "name": "BTC-Context Confluence Trading (Modified)",
                    "philosophy": "Bitcoin provides context. When BTC has clear direction, follow it. When BTC is neutral, trade both sides.",
                    "rules": [
                        "1. Analyze BTC structure first (Daily, 4H, 1H)",
                        "2. If BTC trend strength ≥ 4.0/10 and has clear direction:",
                        "   - Altcoin MUST align with BTC direction",
                        "   - Reject all counter-BTC trades",
                        "3. If BTC is NEUTRAL (strength < 4.0):",
                        "   - Both LONG and SHORT trades allowed",
                        "   - No BTC alignment bonus/penalty (1.0x multiplier)",
                        "4. Apply confluence analysis to all alts",
                        "5. Use BTC-adjusted confluence scores"
                    ],
                    "parameters": {
                        "btc_min_trend_strength": BTC_MIN_TREND_STRENGTH,
                        "target_move": f"{TARGET_PROFIT_RANGE[0]}-{TARGET_PROFIT_RANGE[1]}%",
                        "min_confluence": MIN_CONFLUENCE_SCORE,
                        "max_stop_loss": f"{MAX_STOP_LOSS}%",
                        "min_risk_reward": f"{MIN_RISK_REWARD}:1"
                    },
                    "btc_alignment_multipliers": {
                        "perfect_alignment": "1.3x-1.5x score bonus",
                        "counter_btc": "0.5x score penalty (rejected)",
                        "btc_neutral_both_sides": "1.0x (neutral, both sides allowed)"
                    }
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
    log.info(f"🌐 BTC-context HTTP server started on port {port}")
    
    async with server:
        await server.serve_forever()

# ================ MAIN ================
async def main():
    """Main function"""
    scanner = BTCConfluenceMoveScanner()
    
    http_task = asyncio.create_task(start_confluence_server(scanner))
    
    await scanner.run()

if __name__ == "__main__":
    asyncio.run(main())