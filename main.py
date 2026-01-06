#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CONFLUENCE SCANNER v5.0 - 3-5% MOVE STRATEGY
Multi-layer confluence analysis with precise entry detection
NO TA-Lib DEPENDENCY - Pure Python implementation
OKX EXCHANGE INTEGRATION - No geographical restrictions
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
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 10))   # 10 seconds
TOP_N_VOLUME = int(os.getenv("TOP_N_VOLUME", 50))     # Focus on quality pairs
MIN_VOLUME_USD = 1000000  # $1M minimum for liquidity

# Confluence parameters - TARGET: 3-5% MOVES
TARGET_PROFIT_RANGE = (2.0, 6.0)  # Target 2-6% moves (aim for 3-5%)
MAX_STOP_LOSS = 1.5               # Max 1.5% stop loss
MIN_RISK_REWARD = 2.5             # Minimum 1:2.5 risk/reward
MIN_CONFLUENCE_SCORE = 1.0        # Min confluence score (out of 10)

# Timeframes for multi-layer analysis
TIMEFRAMES = {
    "DAILY": "1d",      # Primary trend direction
    "4H": "4h",         # Key support/resistance
    "1H": "1h",         # Market structure
    "15M": "15m",       # Entry zone & momentum
    "5M": "5m",         # Entry trigger
}

# Confluence scoring weights
CONFLUENCE_WEIGHTS = {
    "market_structure": 0.25,      # Structure alignment
    "order_flow": 0.30,            # Volume & orderbook
    "momentum": 0.25,              # RSI/MACD divergence
    "liquidity": 0.20,             # Liquidity zones
}

# RSI settings for divergence detection
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
    """Pure Python RSI calculation - no TA-Lib needed"""
    delta = prices.diff()
    
    # Separate gains and losses
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    
    # Calculate average gain and loss
    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()
    
    # Handle initial period
    for i in range(period, len(prices)):
        avg_gain.iloc[i] = (avg_gain.iloc[i-1] * (period - 1) + gain.iloc[i]) / period
        avg_loss.iloc[i] = (avg_loss.iloc[i-1] * (period - 1) + loss.iloc[i]) / period
    
    # Calculate RS and RSI
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    
    return rsi

def calculate_macd_pure(prices: pd.Series, fast_period: int = 12, 
                       slow_period: int = 26, signal_period: int = 9) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """Pure Python MACD calculation - no TA-Lib needed"""
    # Calculate EMAs
    ema_fast = prices.ewm(span=fast_period, adjust=False).mean()
    ema_slow = prices.ewm(span=slow_period, adjust=False).mean()
    
    # MACD line
    macd_line = ema_fast - ema_slow
    
    # Signal line (EMA of MACD)
    signal_line = macd_line.ewm(span=signal_period, adjust=False).mean()
    
    # Histogram
    histogram = macd_line - signal_line
    
    return macd_line, signal_line, histogram

def calculate_ema(prices: pd.Series, period: int) -> pd.Series:
    """Calculate EMA - pure Python"""
    return prices.ewm(span=period, adjust=False).mean()

# ================ DATA STRUCTURES ================
@dataclass
class MarketStructure:
    """Market structure analysis - Trend & Key Levels"""
    trend: str                     # BULLISH, BEARISH, RANGING
    higher_timeframe_aligned: bool # Daily/4H aligned
    key_support: float             # Major support level
    key_resistance: float          # Major resistance level
    structure_score: float         # 0-1 structure quality
    
    # Structure details
    swing_highs: List[float]       # Recent swing highs
    swing_lows: List[float]        # Recent swing lows
    breaker_blocks: List[Dict]     # Order blocks / FVGs

@dataclass
class OrderFlow:
    """Order flow & volume profile analysis"""
    volume_profile: Dict           # Volume nodes
    volume_spike: bool             # Volume spike at level
    volume_ratio: float            # Current vs average volume
    
    # Order book analysis
    bid_ask_imbalance: float       # -1 to +1 (bearish to bullish)
    orderbook_depth: float         # Orderbook depth ratio
    
    # Accumulation/distribution
    accumulation_score: float      # 0-1 accumulation
    large_transactions: int        # Large tx count
    
    flow_score: float              # Overall flow score 0-1

@dataclass
class MomentumSignal:
    """Short-term momentum signals"""
    rsi_divergence: str           # BULLISH, BEARISH, NONE
    rsi_value: float              # Current RSI
    rsi_zone: str                 # OVERSOLD, OVERBOUGHT, NEUTRAL
    
    macd_signal: str              # BULLISH_CROSS, BEARISH_CROSS, NONE
    macd_histogram: float         # MACD histogram value
    
    candle_pattern: str           # Pattern name
    pattern_strength: float       # 0-1 pattern strength
    
    momentum_score: float         # Overall momentum score 0-1

@dataclass 
class LiquidityZone:
    """Liquidity & stop hunt zones"""
    zone_type: str               # SWEEP_LOW, SWEEP_HIGH, EQ_HIGH, EQ_LOW
    price_level: float           # Key liquidity level
    distance_pct: float          # Distance from current price
    recently_tested: bool        # Recently tested
    strength: float              # Zone strength 0-1
    
    # Liquidation levels
    liquidation_cluster: Dict    # Nearby liquidations
    stop_hunt_potential: bool    # Stop hunt likely

@dataclass
class ConfluenceSetup:
    """Complete confluence setup for 3-5% move"""
    signal_id: str
    symbol: str
    side: str                    # LONG, SHORT
    
    # Confluence analysis
    market_structure: MarketStructure
    order_flow: OrderFlow
    momentum: MomentumSignal
    liquidity_zone: LiquidityZone
    
    # Entry parameters
    entry_price: float
    entry_type: str              # LIMIT_AT_SUPPORT, BREAKOUT_RETEST, etc.
    entry_confidence: float      # 0-1 entry confidence
    
    # Risk management
    stop_loss: float
    take_profit: float
    risk_pct: float              # % risk
    reward_pct: float            # % reward
    risk_reward: float
    
    # Confluence scoring
    confluence_score: float      # 0-10 overall score
    confluence_details: Dict     # Breakdown of scores
    conditions_met: List[str]    # Which conditions triggered
    
    # Expected move
    expected_move_pct: float     # Expected move percentage (3-5%)
    probability_score: float     # Probability of hitting target
    
    # Timing
    timeframe_used: str          # Primary entry timeframe
    signal_timestamp: float

# ================ PROFESSIONAL LOGGING ================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)8s | %(name)s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger("confluence_scanner")

# ================ CORE CONFLUENCE ENGINE ================
class ConfluenceScanner:
    """Multi-layer confluence scanner for 3-5% moves"""
    
    class SignalManager:
        """Manage signals with confluence-based deduplication"""
        
        def __init__(self):
            self.active_signals = {}      # symbol: signal_id
            self.signal_states = {}       # signal_id: state
            self.confluence_history = {}  # symbol: recent confluence scores
            
        def should_generate_signal(self, symbol: str, new_score: float) -> bool:
            """Check if new signal has significantly better confluence"""
            if symbol not in self.active_signals:
                return True
            
            signal_id = self.active_signals[symbol]
            if signal_id not in self.signal_states:
                return True
            
            state = self.signal_states[signal_id]
            
            # Only allow new signal if:
            # 1. Old signal is CLOSED, OR
            # 2. New confluence score is 15%+ better
            if state.get("status") == "CLOSED":
                return True
            
            old_score = state.get("confluence_score", 0)
            if new_score > old_score * 1.15:  # 15% better confluence
                log.debug(f"{symbol}: New confluence {new_score:.1f} vs old {old_score:.1f}")
                return True
            
            return False
        
        def register_signal(self, signal: ConfluenceSetup):
            """Register new confluence signal"""
            symbol = signal.symbol
            
            # Clear old if exists
            if symbol in self.active_signals:
                old_id = self.active_signals[symbol]
                if old_id in self.signal_states:
                    del self.signal_states[old_id]
            
            # Register new
            self.active_signals[symbol] = signal.signal_id
            self.signal_states[signal.signal_id] = {
                "symbol": symbol,
                "side": signal.side,
                "confluence_score": signal.confluence_score,
                "status": "PENDING",
                "timestamp": signal.signal_timestamp
            }
            
            # Track confluence history
            if symbol not in self.confluence_history:
                self.confluence_history[symbol] = deque(maxlen=10)
            self.confluence_history[symbol].append(signal.confluence_score)
            
            log.debug(f"Registered confluence signal {signal.signal_id[:8]} for {symbol}")
        
        def update_signal_status(self, signal_id: str, status: str):
            """Update signal status"""
            if signal_id in self.signal_states:
                self.signal_states[signal_id]["status"] = status
                log.debug(f"Signal {signal_id[:8]} → {status}")
        
        def cleanup_old_signals(self):
            """Clean up old closed signals"""
            current_time = time.time()
            to_remove = []
            
            for signal_id, state in list(self.signal_states.items()):
                if state.get("status") == "CLOSED":
                    age = current_time - state.get("timestamp", 0)
                    if age > 3600:  # 1 hour
                        to_remove.append(signal_id)
            
            for signal_id in to_remove:
                symbol = self.signal_states[signal_id]["symbol"]
                if self.active_signals.get(symbol) == signal_id:
                    del self.active_signals[symbol]
                del self.signal_states[signal_id]
                log.debug(f"Cleaned signal {signal_id[:8]} for {symbol}")
    
    def __init__(self):
        self.signal_manager = self.SignalManager()
        self.daily_stats = {
            "scans": 0,
            "pairs_analyzed": 0,
            "confluence_signals": 0,
            "high_quality_signals": 0,
            "rejected_low_confluence": 0,
            "rejected_no_alignment": 0
        }
    
    # ========== MARKET STRUCTURE ANALYSIS ==========
    
    def analyze_market_structure(self, df_daily: pd.DataFrame, df_4h: pd.DataFrame, 
                                df_1h: pd.DataFrame) -> MarketStructure:
        """
        Analyze market structure across multiple timeframes
        """
        try:
            if df_daily is None or df_4h is None or df_1h is None:
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
    
    def _determine_trend(self, df: pd.DataFrame, timeframe: str) -> str:
        """Determine trend direction"""
        try:
            if len(df) < 50:
                return "RANGING"
            
            # Use EMAs for trend (pure Python)
            ema_fast = calculate_ema(df['close'], 9).iloc[-1]
            ema_medium = calculate_ema(df['close'], 21).iloc[-1]
            ema_slow = calculate_ema(df['close'], 50).iloc[-1]
            
            # Check alignment
            if ema_fast > ema_medium > ema_slow:
                return "BULLISH"
            elif ema_fast < ema_medium < ema_slow:
                return "BEARISH"
            
            # Check price position relative to EMAs
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
            
            return higher_trend == lower_trend
            
        except Exception as e:
            return False
    
    def _identify_key_levels(self, df: pd.DataFrame) -> Tuple[float, float]:
        """Identify key support and resistance levels"""
        try:
            if len(df) < 30:
                return 0.0, 0.0
            
            # Use recent price action for key levels
            recent_highs = df['high'].iloc[-30:].nlargest(3).values
            recent_lows = df['low'].iloc[-30:].nsmallest(3).values
            
            # Major resistance (cluster of highs)
            key_resistance = np.mean(recent_highs) if len(recent_highs) > 0 else 0.0
            
            # Major support (cluster of lows)
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
            
            # Simple swing point detection
            for i in range(2, len(df) - 2):
                if i < 5:  # Skip too recent
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
            
            return swing_highs[-3:], swing_lows[-3:]  # Return last 3
            
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
                
                # Bearish order block (rally base drop)
                if (prev['close'] > prev['open'] and  # Bullish candle
                    current['close'] < current['open'] and  # Bearish candle
                    current['low'] < prev['low']):  # Takes out low
                    
                    block = {
                        "type": "BEARISH_OB",
                        "high": float(prev['high']),
                        "low": float(prev['low']),
                        "index": i
                    }
                    blocks.append(block)
                
                # Bullish order block (drop base rally)
                if (prev['close'] < prev['open'] and  # Bearish candle
                    current['close'] > current['open'] and  # Bullish candle
                    current['high'] > prev['high']):  # Takes out high
                    
                    block = {
                        "type": "BULLISH_OB",
                        "high": float(prev['high']),
                        "low": float(prev['low']),
                        "index": i
                    }
                    blocks.append(block)
            
            return blocks[-5:]  # Return last 5 blocks
            
        except Exception as e:
            return []
    
    def _calculate_structure_score(self, trend: str, aligned: bool, 
                                  swing_highs: List, swing_lows: List) -> float:
        """Calculate market structure quality score"""
        score = 0.0
        
        # Trend strength (0-0.3)
        if trend != "RANGING":
            score += 0.3
        
        # HTF alignment (0-0.3)
        if aligned:
            score += 0.3
        
        # Clear swing points (0-0.4)
        if len(swing_highs) >= 2 and len(swing_lows) >= 2:
            score += 0.4
        
        return min(score, 1.0)
    
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
            if df_15m is None or len(df_15m) < 30:
                return self._get_default_order_flow()
            
            # 1. Volume profile analysis
            volume_profile = self._analyze_volume_profile(df_15m)
            
            # 2. Volume spike detection
            volume_spike, volume_ratio = self._detect_volume_spike(df_15m)
            
            # 3. Bid/ask imbalance (simulated)
            bid_ask_imbalance = self._estimate_bid_ask_imbalance(df_15m)
            
            # 4. Orderbook depth (simulated)
            orderbook_depth = self._estimate_orderbook_depth(df_15m)
            
            # 5. Accumulation score
            accumulation_score = self._calculate_accumulation_score(df_15m)
            
            # 6. Large transactions (simulated)
            large_transactions = self._count_large_transactions(df_15m)
            
            # 7. Overall flow score
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
            
            # Simple volume profile
            price_bins = np.linspace(df['low'].min(), df['high'].max(), 20)
            volume_by_price = []
            
            for i in range(len(price_bins) - 1):
                low_bin = price_bins[i]
                high_bin = price_bins[i+1]
                
                # Volume in this price range
                mask = (df['low'] >= low_bin) & (df['high'] <= high_bin)
                volume_in_range = df.loc[mask, 'volume'].sum()
                
                volume_by_price.append({
                    "price_range": (float(low_bin), float(high_bin)),
                    "volume": float(volume_in_range)
                })
            
            # Find high volume nodes (POC areas)
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
                "high_volume_nodes": high_volume_nodes[:3],  # Top 3
                "low_volume_gaps": []  # Simplified
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
                spike = ratio >= 2.0  # 2x volume spike
                return spike, float(ratio)
            
            return False, 1.0
            
        except Exception as e:
            return False, 1.0
    
    def _estimate_bid_ask_imbalance(self, df: pd.DataFrame) -> float:
        """Estimate bid/ask imbalance from price action"""
        try:
            if len(df) < 10:
                return 0.0
            
            # Use close vs open to estimate buying/selling pressure
            recent = df.iloc[-5:]
            closes = recent['close'].values
            opens = recent['open'].values
            
            # Count bullish vs bearish candles
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
            
            # Lower volatility suggests better depth
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
            
            # Using price-volume relationship
            price_change = df['close'].iloc[-1] - df['close'].iloc[-10]
            volume_change = df['volume'].iloc[-10:].mean() / df['volume'].iloc[-30:-10].mean()
            
            # Positive price with increasing volume = accumulation
            if price_change > 0 and volume_change > 1.2:
                return 0.8
            # Negative price with decreasing volume = possible accumulation
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
            
            # Simulate based on volume spikes
            avg_volume = df['volume'].mean()
            large_tx_count = sum(df['volume'] > avg_volume * 3)
            
            return min(large_tx_count, 10)
            
        except Exception as e:
            return 0
    
    def _calculate_flow_score(self, volume_spike: bool, volume_ratio: float,
                             imbalance: float, depth: float, accumulation: float) -> float:
        """Calculate overall flow score"""
        weights = [0.25, 0.20, 0.25, 0.15, 0.15]
        
        # Volume spike score
        spike_score = 1.0 if volume_spike else 0.5
        
        # Volume ratio score (normalized)
        volume_score = min(volume_ratio / 3.0, 1.0)
        
        # Imbalance score (absolute value)
        imbalance_score = abs(imbalance)
        
        # Depth score
        depth_score = depth
        
        # Accumulation score
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
    
    # ========== MOMENTUM ANALYSIS (NO TA-Lib) ==========
    
    def analyze_momentum(self, df_15m: pd.DataFrame, df_5m: pd.DataFrame) -> MomentumSignal:
        """Analyze short-term momentum signals - NO TA-Lib"""
        try:
            if df_15m is None or df_5m is None:
                return self._get_default_momentum()
            
            # 1. RSI analysis (pure Python)
            rsi_divergence, rsi_value, rsi_zone = self._analyze_rsi_pure(df_15m)
            
            # 2. MACD analysis (pure Python)
            macd_signal, macd_histogram = self._analyze_macd_pure(df_15m)
            
            # 3. Candlestick patterns
            candle_pattern, pattern_strength = self._analyze_candle_patterns(df_5m)
            
            # 4. Momentum score
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
        """Analyze RSI for divergence and zones - Pure Python"""
        try:
            if len(df) < 30:
                return "NONE", 50.0, "NEUTRAL"
            
            # Calculate RSI using pure Python
            prices = df['close']
            rsi_values = calculate_rsi_pure(prices, period=RSI_PERIOD)
            
            if len(rsi_values) < 5 or np.isnan(rsi_values.iloc[-1]):
                return "NONE", 50.0, "NEUTRAL"
            
            current_rsi = float(rsi_values.iloc[-1])
            
            # Determine RSI zone
            if current_rsi <= RSI_OVERSOLD:
                rsi_zone = "OVERSOLD"
            elif current_rsi >= RSI_OVERBOUGHT:
                rsi_zone = "OVERBOUGHT"
            else:
                rsi_zone = "NEUTRAL"
            
            # Check for divergence (simplified)
            rsi_divergence = "NONE"
            
            # Look for hidden bullish divergence (RSI making higher low while price makes lower low)
            if len(rsi_values) >= 10:
                recent_rsi = rsi_values.iloc[-5:].values
                recent_prices = prices.iloc[-5:].values
                
                if (recent_rsi[-1] > recent_rsi[-3] and  # RSI higher low
                    recent_prices[-1] < recent_prices[-3]):  # Price lower low
                    rsi_divergence = "BULLISH_HIDDEN"
                
                # Hidden bearish divergence
                elif (recent_rsi[-1] < recent_rsi[-3] and  # RSI lower high
                      recent_prices[-1] > recent_prices[-3]):  # Price higher high
                    rsi_divergence = "BEARISH_HIDDEN"
            
            return rsi_divergence, current_rsi, rsi_zone
            
        except Exception as e:
            return "NONE", 50.0, "NEUTRAL"
    
    def _analyze_macd_pure(self, df: pd.DataFrame) -> Tuple[str, float]:
        """Analyze MACD signals - Pure Python"""
        try:
            if len(df) < 35:
                return "NONE", 0.0
            
            # Calculate MACD using pure Python
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
            
            # Check for crossover
            current_macd = float(macd_line.iloc[-1])
            current_signal = float(signal_line.iloc[-1])
            prev_macd = float(macd_line.iloc[-2]) if len(macd_line) > 1 else 0.0
            prev_signal = float(signal_line.iloc[-2]) if len(signal_line) > 1 else 0.0
            
            macd_signal = "NONE"
            
            # Bullish crossover
            if prev_macd < prev_signal and current_macd > current_signal:
                macd_signal = "BULLISH_CROSS"
            # Bearish crossover
            elif prev_macd > prev_signal and current_macd < current_signal:
                macd_signal = "BEARISH_CROSS"
            # Histogram flip
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
            
            # Get recent candles
            current = df.iloc[-1]
            prev1 = df.iloc[-2]
            prev2 = df.iloc[-3] if len(df) >= 3 else None
            
            # Bullish engulfing
            if (prev1['close'] < prev1['open'] and  # Previous bearish
                current['close'] > current['open'] and  # Current bullish
                current['close'] > prev1['open'] and  # Engulfs previous
                current['open'] < prev1['close']):
                return "BULLISH_ENGULFING", 0.8
            
            # Bearish engulfing
            if (prev1['close'] > prev1['open'] and  # Previous bullish
                current['close'] < current['open'] and  # Current bearish
                current['close'] < prev1['open'] and  # Engulfs previous
                current['open'] > prev1['close']):
                return "BEARISH_ENGULFING", 0.8
            
            # Hammer (bullish reversal)
            if (current['close'] > current['open'] and  # Bullish
                (current['low'] - min(current['open'], current['close'])) > 
                (abs(current['close'] - current['open']) * 2) and  # Long lower wick
                (current['high'] - max(current['open'], current['close'])) < 
                abs(current['close'] - current['open'])):  # Small upper wick
                return "HAMMER", 0.7
            
            # Shooting star (bearish reversal)
            if (current['close'] < current['open'] and  # Bearish
                (current['high'] - max(current['open'], current['close'])) > 
                (abs(current['close'] - current['open']) * 2) and  # Long upper wick
                (min(current['open'], current['close']) - current['low']) < 
                abs(current['close'] - current['open'])):  # Small lower wick
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
        
        # RSI divergence (0-0.4)
        if rsi_div != "NONE":
            score += 0.4
        elif rsi_zone in ["OVERSOLD", "OVERBOUGHT"]:
            score += 0.2
        
        # MACD signal (0-0.3)
        if macd_signal != "NONE":
            score += 0.3
        
        # Candlestick pattern (0-0.3)
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
            if df_4h is None or df_1h is None:
                return self._get_default_liquidity_zone()
            
            # 1. Identify liquidity zones
            best_zone = self._identify_liquidity_zone(df_4h, df_1h, current_price)
            
            # 2. Calculate distance
            if best_zone["price_level"] > 0:
                distance_pct = abs(current_price - best_zone["price_level"]) / current_price * 100
            else:
                distance_pct = 0.0
            
            # 3. Check if recently tested
            recently_tested = self._check_recent_test(df_1h, best_zone["price_level"])
            
            # 4. Liquidation clusters (simulated)
            liquidation_cluster = self._estimate_liquidations(df_1h, best_zone["price_level"])
            
            # 5. Stop hunt potential
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
            # 1. Recent swing high/low (4H)
            recent_high_4h = df_4h['high'].iloc[-20:].max()
            recent_low_4h = df_4h['low'].iloc[-20:].min()
            
            # Equal highs/lows
            if abs(current_price - recent_high_4h) / recent_high_4h < 0.01:  # 1%
                zones.append({
                    "zone_type": "EQ_HIGH",
                    "price_level": float(recent_high_4h),
                    "strength": 0.8
                })
            
            if abs(current_price - recent_low_4h) / recent_low_4h < 0.01:  # 1%
                zones.append({
                    "zone_type": "EQ_LOW", 
                    "price_level": float(recent_low_4h),
                    "strength": 0.8
                })
            
            # 2. Previous liquidity sweeps (1H)
            # Look for wicks that took out previous highs/lows
            if len(df_1h) >= 10:
                for i in range(5, len(df_1h) - 1):
                    candle = df_1h.iloc[i]
                    prev_high = df_1h['high'].iloc[i-5:i].max()
                    prev_low = df_1h['low'].iloc[i-5:i].min()
                    
                    # Sweep high
                    if candle['high'] > prev_high * 1.005:  # 0.5% above
                        zones.append({
                            "zone_type": "SWEEP_HIGH",
                            "price_level": float(candle['high']),
                            "strength": 0.9
                        })
                    
                    # Sweep low
                    if candle['low'] < prev_low * 0.995:  # 0.5% below
                        zones.append({
                            "zone_type": "SWEEP_LOW",
                            "price_level": float(candle['low']),
                            "strength": 0.9
                        })
            
            # 3. Select strongest zone near price
            if zones:
                # Sort by strength and proximity
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
            
            # Simulate based on volatility and price action
            volatility = df['close'].pct_change().std() * 100
            
            # More liquidations near swing points
            recent_high = df['high'].iloc[-10:].max()
            recent_low = df['low'].iloc[-10:].min()
            
            if abs(price_level - recent_high) / recent_high < 0.01:
                # Near high = more short liquidations if broken
                return {"longs": 50, "shorts": 150, "total": 200}
            elif abs(price_level - recent_low) / recent_low < 0.01:
                # Near low = more long liquidations if broken
                return {"longs": 150, "shorts": 50, "total": 200}
            else:
                # Neutral zone
                return {"longs": 100, "shorts": 100, "total": 200}
                
        except Exception as e:
            return {"longs": 0, "shorts": 0, "total": 0}
    
    def _assess_stop_hunt_potential(self, zone_type: str, distance_pct: float, 
                                   liquidations: Dict) -> bool:
        """Assess stop hunt potential"""
        if zone_type == "NONE":
            return False
        
        # Close to level with liquidations nearby
        if distance_pct < 1.0 and liquidations.get("total", 0) > 100:
            return True
        
        # Sweep zones always have potential
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
    
    # ========== CONFLUENCE SIGNAL GENERATION ==========
    
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
            "trend": structure.trend,
            "aligned": structure.higher_timeframe_aligned,
            "raw_score": structure.structure_score
        }
        
        # 2. Order Flow Score (0-3.0)
        flow_score = order_flow.flow_score * 3.0
        scores["order_flow"] = flow_score
        details["order_flow"] = {
            "volume_spike": order_flow.volume_spike,
            "volume_ratio": order_flow.volume_ratio,
            "imbalance": order_flow.bid_ask_imbalance,
            "raw_score": order_flow.flow_score
        }
        
        # 3. Momentum Score (0-2.5)
        mom_score = momentum.momentum_score * 2.5
        scores["momentum"] = mom_score
        details["momentum"] = {
            "rsi_divergence": momentum.rsi_divergence,
            "rsi_zone": momentum.rsi_zone,
            "macd_signal": momentum.macd_signal,
            "candle_pattern": momentum.candle_pattern,
            "raw_score": momentum.momentum_score
        }
        
        # 4. Liquidity Score (0-2.0)
        if liquidity.zone_type != "NONE":
            liq_score = liquidity.strength * 2.0
            if liquidity.stop_hunt_potential:
                liq_score *= 1.2  # Bonus for stop hunt potential
        else:
            liq_score = 0.0
        
        scores["liquidity"] = liq_score
        details["liquidity"] = {
            "zone_type": liquidity.zone_type,
            "stop_hunt_potential": liquidity.stop_hunt_potential,
            "strength": liquidity.strength
        }
        
        # 5. Side-specific adjustments
        side_adjustment = 0.0
        if side == "LONG":
            # Bonus for bullish confluence
            if (structure.trend == "BULLISH" and 
                momentum.rsi_divergence == "BULLISH_HIDDEN" and
                momentum.macd_signal in ["BULLISH_CROSS", "BULLISH_FLIP"]):
                side_adjustment = 0.5
        
        elif side == "SHORT":
            # Bonus for bearish confluence
            if (structure.trend == "BEARISH" and
                momentum.rsi_divergence == "BEARISH_HIDDEN" and
                momentum.macd_signal in ["BEARISH_CROSS", "BEARISH_FLIP"]):
                side_adjustment = 0.5
        
        scores["side_adjustment"] = side_adjustment
        
        # Calculate total score (0-10)
        total_score = sum(scores.values())
        
        # Ensure max 10
        total_score = min(total_score, 10.0)
        
        return total_score, {
            "scores": scores,
            "details": details,
            "total": total_score
        }
    
    def determine_entry_type(self, structure: MarketStructure, momentum: MomentumSignal,
                           liquidity: LiquidityZone, side: str) -> str:
        """Determine entry type based on confluence"""
        
        # Check for breakout retest
        if liquidity.recently_tested and "SWEEP" in liquidity.zone_type:
            return "BREAKOUT_RETEST" if side == "LONG" else "BREAKDOWN_RETEST"
        
        # Check for order block entry
        if structure.breaker_blocks:
            last_block = structure.breaker_blocks[-1]
            if side == "LONG" and last_block["type"] == "BULLISH_OB":
                return "ORDER_BLOCK_ENTRY"
            elif side == "SHORT" and last_block["type"] == "BEARISH_OB":
                return "ORDER_BLOCK_ENTRY"
        
        # Check for support/resistance bounce
        if momentum.candle_pattern in ["HAMMER", "BULLISH_ENGULFING", "INSIDE_BAR"]:
            return "SUPPORT_BOUNCE" if side == "LONG" else "RESISTANCE_BOUNCE"
        
        # Default
        return "CONFLUENCE_ZONE_ENTRY"
    
    def calculate_entry_confidence(self, confluence_score: float, 
                                  order_flow: OrderFlow, momentum: MomentumSignal) -> float:
        """Calculate entry confidence (0-1)"""
        base_confidence = confluence_score / 10.0
        
        # Volume confirmation bonus
        if order_flow.volume_spike:
            base_confidence *= 1.2
        
        # Strong momentum bonus
        if momentum.momentum_score > 0.7:
            base_confidence *= 1.1
        
        # RSI divergence bonus
        if momentum.rsi_divergence != "NONE":
            base_confidence *= 1.15
        
        return min(base_confidence, 1.0)
    
    def generate_confluence_signal(self, multi_tf_data: Dict[str, pd.DataFrame],
                                 symbol: str) -> Optional[ConfluenceSetup]:
        """
        Generate confluence-based signal for 3-5% moves
        """
        try:
            # Get timeframe data
            df_daily = multi_tf_data.get("DAILY")
            df_4h = multi_tf_data.get("4H")
            df_1h = multi_tf_data.get("1H")
            df_15m = multi_tf_data.get("15M")
            df_5m = multi_tf_data.get("5M")
            
            # Check data
            if None in [df_daily, df_4h, df_1h, df_15m, df_5m]:
                log.debug(f"{symbol}: Missing timeframe data")
                return None
            
            # Get current price from 5M
            current_price = df_5m['close'].iloc[-1]
            
            # 1. Analyze market structure
            market_structure = self.analyze_market_structure(df_daily, df_4h, df_1h)
            
            # CRITICAL: Need clear structure
            if market_structure.trend == "RANGING":
                self.daily_stats["rejected_no_alignment"] += 1
                log.debug(f"{symbol}: No clear trend/ranging")
                return None
            
            # 2. Analyze order flow
            order_flow = self.analyze_order_flow(df_15m, current_price)
            
            # 3. Analyze momentum
            momentum = self.analyze_momentum(df_15m, df_5m)
            
            # 4. Analyze liquidity zones
            liquidity_zone = self.analyze_liquidity_zones(df_4h, df_1h, current_price)
            
            # 5. Determine trade side based on confluence
            side = self._determine_trade_side(
                market_structure, momentum, liquidity_zone
            )
            
            if not side:
                log.debug(f"{symbol}: No clear trade side from confluence")
                return None
            
            # 6. Calculate confluence score
            confluence_score, confluence_details = self.calculate_confluence_score(
                market_structure, order_flow, momentum, liquidity_zone, side
            )
            
            # CRITICAL: Minimum confluence score
            if confluence_score < MIN_CONFLUENCE_SCORE:
                self.daily_stats["rejected_low_confluence"] += 1
                log.debug(f"{symbol}: Low confluence {confluence_score:.1f}/10")
                return None
            
            # 7. Confluence-based deduplication
            if not self.signal_manager.should_generate_signal(symbol, confluence_score):
                return None
            
            # 8. Determine entry parameters
            entry_type = self.determine_entry_type(
                market_structure, momentum, liquidity_zone, side
            )
            
            # 9. Calculate entry price based on zone
            entry_price = self._calculate_entry_price(
                side, market_structure, liquidity_zone, current_price
            )
            
            # 10. Calculate stop loss
            stop_loss = self._calculate_stop_loss(
                side, entry_price, market_structure, liquidity_zone
            )
            
            # 11. Calculate take profit for 3-5% move
            take_profit, expected_move_pct = self._calculate_take_profit(
                side, entry_price, market_structure, confluence_score
            )
            
            # 12. Calculate risk/reward
            risk = abs(entry_price - stop_loss)
            reward = abs(take_profit - entry_price)
            
            if risk == 0:
                return None
            
            risk_reward = reward / risk
            risk_pct = risk / entry_price * 100
            reward_pct = reward / entry_price * 100
            
            # CRITICAL: Minimum risk/reward
            if risk_reward < MIN_RISK_REWARD:
                log.debug(f"{symbol}: R:R too low {risk_reward:.1f}:1")
                return None
            
            # 13. Calculate entry confidence
            entry_confidence = self.calculate_entry_confidence(
                confluence_score, order_flow, momentum
            )
            
            # 14. Determine conditions met
            conditions_met = self._get_confluence_conditions(
                market_structure, order_flow, momentum, liquidity_zone, side
            )
            
            # 15. Probability score based on confluence
            probability_score = min(confluence_score / 10.0 * 1.2, 0.95)  # Max 95%
            
            # 16. Create signal ID
            signal_id = hashlib.md5(
                f"{symbol}:{side}:{confluence_score}:{time.time()}".encode()
            ).hexdigest()
            
            # 17. Create final signal
            signal = ConfluenceSetup(
                signal_id=signal_id,
                symbol=symbol,
                side=side,
                
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
                
                confluence_score=confluence_score,
                confluence_details=confluence_details,
                conditions_met=conditions_met,
                
                expected_move_pct=expected_move_pct,
                probability_score=probability_score,
                
                timeframe_used="15M",
                signal_timestamp=time.time()
            )
            
            # 18. Register signal
            self.signal_manager.register_signal(signal)
            
            # 19. Update stats
            self.daily_stats["confluence_signals"] += 1
            if confluence_score >= 8.0:
                self.daily_stats["high_quality_signals"] += 1
            
            log.info(f"🎯 CONFLUENCE SIGNAL: {symbol} {side} @ {entry_price:.4f}")
            log.info(f"   Confluence: {confluence_score:.1f}/10 | R:R: {risk_reward:.1f}:1")
            log.info(f"   Expected: {expected_move_pct:.1f}% | Confidence: {entry_confidence:.1%}")
            log.info(f"   Structure: {market_structure.trend} | Flow: {order_flow.flow_score:.2f}")
            log.info(f"   Momentum: {momentum.rsi_divergence} | Liquidity: {liquidity_zone.zone_type}")
            
            return signal
            
        except Exception as e:
            log.error(f"Confluence signal error for {symbol}: {e}")
            return None
    
    def _determine_trade_side(self, structure: MarketStructure, 
                             momentum: MomentumSignal, 
                             liquidity: LiquidityZone) -> Optional[str]:
        """Determine trade side based on confluence alignment"""
        
        bullish_factors = 0
        bearish_factors = 0
        
        # Market structure
        if structure.trend == "BULLISH":
            bullish_factors += 2
        elif structure.trend == "BEARISH":
            bearish_factors += 2
        
        # HTF alignment
        if structure.higher_timeframe_aligned:
            if structure.trend == "BULLISH":
                bullish_factors += 1
            elif structure.trend == "BEARISH":
                bearish_factors += 1
        
        # Momentum
        if momentum.rsi_divergence == "BULLISH_HIDDEN":
            bullish_factors += 2
        elif momentum.rsi_divergence == "BEARISH_HIDDEN":
            bearish_factors += 2
        
        if momentum.macd_signal in ["BULLISH_CROSS", "BULLISH_FLIP"]:
            bullish_factors += 1
        elif momentum.macd_signal in ["BEARISH_CROSS", "BEARISH_FLIP"]:
            bearish_factors += 1
        
        # Liquidity zones
        if liquidity.zone_type in ["SWEEP_LOW", "EQ_LOW"]:
            bullish_factors += 1
        elif liquidity.zone_type in ["SWEEP_HIGH", "EQ_HIGH"]:
            bearish_factors += 1
        
        # Determine side with clear majority
        if bullish_factors >= 3 and bullish_factors > bearish_factors:
            return "LONG"
        elif bearish_factors >= 3 and bearish_factors > bullish_factors:
            return "SHORT"
        
        return None
    
    def _calculate_entry_price(self, side: str, structure: MarketStructure,
                              liquidity: LiquidityZone, current_price: float) -> float:
        """Calculate precise entry price"""
        
        # Use liquidity zone if valid
        if liquidity.zone_type != "NONE" and liquidity.price_level > 0:
            zone_price = liquidity.price_level
            
            if side == "LONG":
                # For LONG, enter slightly above support zone
                return zone_price * 1.001  # 0.1% above
            else:
                # For SHORT, enter slightly below resistance zone
                return zone_price * 0.999  # 0.1% below
        
        # Use key levels as fallback
        if side == "LONG" and structure.key_support > 0:
            return structure.key_support * 1.001
        elif side == "SHORT" and structure.key_resistance > 0:
            return structure.key_resistance * 0.999
        
        # Fallback to current price
        return current_price
    
    def _calculate_stop_loss(self, side: str, entry_price: float,
                            structure: MarketStructure, liquidity: LiquidityZone) -> float:
        """Calculate stop loss based on structure"""
        
        # Dynamic stop loss calculation
        if side == "LONG":
            # Look for recent swing low
            if structure.swing_lows:
                recent_low = min(structure.swing_lows)
                stop_loss = min(recent_low * 0.995, entry_price * 0.985)
            else:
                stop_loss = entry_price * 0.985  # 1.5% stop
            
            # Ensure not beyond max stop
            max_sl = entry_price * (1 - MAX_STOP_LOSS / 100)
            stop_loss = min(stop_loss, max_sl)
        
        else:  # SHORT
            # Look for recent swing high
            if structure.swing_highs:
                recent_high = max(structure.swing_highs)
                stop_loss = max(recent_high * 1.005, entry_price * 1.015)
            else:
                stop_loss = entry_price * 1.015  # 1.5% stop
            
            # Ensure not beyond max stop
            min_sl = entry_price * (1 + MAX_STOP_LOSS / 100)
            stop_loss = max(stop_loss, min_sl)
        
        return stop_loss
    
    def _calculate_take_profit(self, side: str, entry_price: float,
                              structure: MarketStructure, confluence_score: float) -> Tuple[float, float]:
        """Calculate take profit for 3-5% move"""
        
        # Base target based on confluence score
        base_target_pct = TARGET_PROFIT_RANGE[0] + (
            (confluence_score - MIN_CONFLUENCE_SCORE) / 
            (10 - MIN_CONFLUENCE_SCORE) * 
            (TARGET_PROFIT_RANGE[1] - TARGET_PROFIT_RANGE[0])
        )
        
        # Ensure within range
        target_pct = max(TARGET_PROFIT_RANGE[0], 
                        min(base_target_pct, TARGET_PROFIT_RANGE[1]))
        
        # Adjust based on structure
        if structure.key_resistance > 0 and side == "LONG":
            # Check if resistance is within reasonable distance
            resistance_pct = (structure.key_resistance - entry_price) / entry_price * 100
            if 2.0 <= resistance_pct <= 8.0:
                target_pct = min(target_pct, resistance_pct * 0.8)  # Target 80% of resistance
        
        elif structure.key_support > 0 and side == "SHORT":
            # Check if support is within reasonable distance
            support_pct = (entry_price - structure.key_support) / entry_price * 100
            if 2.0 <= support_pct <= 8.0:
                target_pct = min(target_pct, support_pct * 0.8)  # Target 80% of support
        
        # Calculate take profit price
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
        
        # Structure conditions
        conditions.append(f"TREND_{structure.trend}")
        if structure.higher_timeframe_aligned:
            conditions.append("HTF_ALIGNED")
        
        # Order flow conditions
        if order_flow.volume_spike:
            conditions.append("VOLUME_SPIKE")
        if order_flow.bid_ask_imbalance > 0.3:
            conditions.append("BID_IMBALANCE")
        elif order_flow.bid_ask_imbalance < -0.3:
            conditions.append("ASK_IMBALANCE")
        
        # Momentum conditions
        if momentum.rsi_divergence != "NONE":
            conditions.append(f"RSI_{momentum.rsi_divergence}")
        if momentum.macd_signal != "NONE":
            conditions.append(f"MACD_{momentum.macd_signal}")
        if momentum.candle_pattern != "NONE":
            conditions.append(f"CANDLE_{momentum.candle_pattern}")
        
        # Liquidity conditions
        if liquidity.zone_type != "NONE":
            conditions.append(f"LIQ_{liquidity.zone_type}")
        if liquidity.stop_hunt_potential:
            conditions.append("STOP_HUNT_POTENTIAL")
        
        # Side-specific conditions
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
class ConfluenceMoveScanner:
    """Main scanner for 3-5% confluence moves with OKX exchange"""
    
    def __init__(self):
        self.scanner = ConfluenceScanner()
        self.exchange = None
        self.db = None
        self.scan_cycle = 0
        
    async def initialize(self):
        """Initialize the scanner"""
        log.info("=" * 70)
        log.info("🎯 CONFLUENCE SCANNER v5.0 - 3-5% MOVE STRATEGY")
        log.info("=" * 70)
        log.info("EXCHANGE: OKX (No geographical restrictions)")
        log.info("STRATEGY: Multi-layer confluence analysis")
        log.info("TARGET: 3-5% directional moves")
        log.info("ANALYSIS LAYERS:")
        log.info("  1. Market Structure (25%) - Trend & Key Levels")
        log.info("  2. Order Flow (30%) - Volume & Orderbook")
        log.info("  3. Momentum (25%) - RSI/MACD divergence")
        log.info("  4. Liquidity (20%) - Stop hunts & Zones")
        log.info(f"MIN CONFLUENCE: {MIN_CONFLUENCE_SCORE}/10")
        log.info(f"TARGET RANGE: {TARGET_PROFIT_RANGE[0]}-{TARGET_PROFIT_RANGE[1]}%")
        log.info(f"RISK/REWARD: {MIN_RISK_REWARD}:1 minimum")
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
            
            # Confluence signals table
            await self.db.execute("""
            CREATE TABLE IF NOT EXISTS confluence_signals (
                id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                entry_price REAL NOT NULL,
                stop_loss REAL NOT NULL,
                take_profit REAL NOT NULL,
                
                confluence_score REAL NOT NULL,
                confluence_details TEXT,
                conditions_met TEXT,
                
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
            
            # Performance tracking
            await self.db.execute("""
            CREATE TABLE IF NOT EXISTS confluence_performance (
                date DATE PRIMARY KEY,
                total_signals INTEGER,
                high_quality_signals INTEGER,
                avg_confluence_score REAL,
                avg_expected_move REAL,
                win_rate REAL,
                avg_pnl REAL,
                total_pnl REAL
            )
            """)
            
            await self.db.commit()
            log.info("✅ Database initialized")
            
        except Exception as e:
            log.error(f"Database error: {e}")
            raise
    
    async def _init_exchange(self):
        """Initialize OKX exchange connection"""
        try:
            # Primary exchange: OKX
            self.exchange = ccxt.okx({
                "enableRateLimit": True,
                "options": {
                    "defaultType": "spot",
                    "fetchMarkets": "spot",
                    "adjustForTimeDifference": True,
                },
                "timeout": 30000,  # 30 second timeout
                "rateLimit": 20,   # OKX rate limit is 20 req/sec for public
            })
            
            # Test connection with market fetch
            markets = await self.exchange.fetch_markets(params={'type': 'spot'})
            
            # Filter for USDT pairs only
            usdt_pairs = [m['symbol'] for m in markets if m['symbol'].endswith('/USDT')]
            
            log.info(f"✅ OKX exchange connected. Found {len(usdt_pairs)} USDT pairs")
            
            # Get BTC price to verify
            ticker = await self.exchange.fetch_ticker("BTC/USDT")
            log.info(f"📊 BTC/USDT: ${ticker['last']:.2f}")
            
        except Exception as e:
            log.error(f"OKX exchange error: {e}")
            log.info("⚠️ Trying alternative exchange: Bybit...")
            
            # Fallback to Bybit
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
            message = f"""
🎯 <b>CONFLUENCE SCANNER v5.0 - 3-5% MOVE STRATEGY</b>

<b>📊 EXCHANGE:</b> OKX (No geographical restrictions)

<b>🧠 ANALYSIS FRAMEWORK (4 LAYERS):</b>
1️⃣ <b>Market Structure (25%)</b>
‎   • Trend direction (Daily/4H)
‎   • Key support/resistance levels
‎   • HTF alignment confirmation
‎   • Swing point analysis

2️⃣ <b>Order Flow (30%)</b>
‎   • Volume profile & spikes
‎   • Bid/ask imbalance
‎   • Orderbook depth
‎   • Accumulation/distribution

3️⃣ <b>Momentum (25%)</b>
‎   • RSI hidden divergence
‎   • MACD histogram flips
‎   • Candlestick patterns
‎   • Short-term momentum shifts

4️⃣ <b>Liquidity (20%)</b>
‎   • Liquidity zone identification
‎   • Stop hunt potential
‎   • Sweep highs/lows
‎   • Equal highs/lows

<b>🎯 TARGET PARAMETERS:</b>
‎• Minimum Confluence: {MIN_CONFLUENCE_SCORE}/10
‎• Expected Move: {TARGET_PROFIT_RANGE[0]}-{TARGET_PROFIT_RANGE[1]}%
‎• Max Stop Loss: {MAX_STOP_LOSS}%
‎• Min Risk/Reward: {MIN_RISK_REWARD}:1
‎• Entry Confidence: Based on confluence alignment

<b>⚡ ENTRY TYPES:</b>
‎• Breakout/breakdown retests
‎• Order block entries
‎• Support/resistance bounces
‎• Confluence zone entries

<b>📈 SCANNING SETTINGS:</b>
‎• Interval: {SCAN_INTERVAL}s
‎• Top pairs: {TOP_N_VOLUME}
‎• Min volume: ${MIN_VOLUME_USD:,.0f}
‎• Timeframes: Daily → 5M

<b>🛡️ RISK MANAGEMENT:</b>
‎• One signal per symbol
‎• Confluence-based deduplication
‎• Dynamic stop losses
‎• Asymmetric payoff focus

The scanner hunts for setups where all 4 layers align, providing high-probability 3-5% move opportunities.

#ConfluenceTrading #HighProbability #35PercentMoves #OKX
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
    
    async def _fetch_single_timeframe(self, symbol: str, timeframe: str, 
                                     limit: int, tf_name: str) -> pd.DataFrame:
        """Fetch single timeframe data"""
        try:
            # OKX specific parameters
            params = {'type': 'spot'}
            
            ohlcv = await self.exchange.fetch_ohlcv(
                symbol, 
                timeframe=timeframe, 
                limit=limit,
                params=params
            )
            
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
                    return df
            
            return pd.DataFrame()
            
        except Exception as e:
            log.debug(f"OHLCV error {symbol} {tf_name}: {str(e)[:50]}")
            return pd.DataFrame()
    
    async def fetch_timeframe_data(self, symbol: str) -> Dict[str, pd.DataFrame]:
        """Batch fetch OHLCV data for multiple timeframes"""
        data = {}
        tasks = []
        
        # Adjust limits for OKX (better historical data)
        limit_map = {
            "DAILY": 100,
            "4H": 120,    # 20 days of 4H
            "1H": 168,    # 7 days of hourly
            "15M": 96,    # 24 hours of 15m
            "5M": 72      # 6 hours of 5m
        }
        
        for tf_name, tf in TIMEFRAMES.items():
            limit = limit_map.get(tf_name, 50)
            tasks.append(self._fetch_single_timeframe(symbol, tf, limit, tf_name))
        
        # Fetch all timeframes concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for tf_name, result in zip(TIMEFRAMES.keys(), results):
            if isinstance(result, pd.DataFrame) and not result.empty:
                data[tf_name] = result
            else:
                log.debug(f"No data for {symbol} {tf_name}")
        
        return data
    
    async def get_active_pairs(self) -> List[Tuple[str, float]]:
        """Get active trading pairs from OKX"""
        try:
            # OKX supports params for market type
            markets = await self.exchange.fetch_markets(params={'type': 'spot'})
            
            active_pairs = []
            
            for market in markets:
                symbol = market['symbol']
                
                # Filter for USDT pairs only
                if symbol.endswith('/USDT'):
                    # Get ticker for volume data
                    try:
                        ticker = await self.exchange.fetch_ticker(symbol)
                        volume = ticker.get('quoteVolume', 0)
                        
                        if volume >= MIN_VOLUME_USD:
                            # Check price for minimum movement potential
                            price = ticker.get('last', 0)
                            if price > 0.01:  # Avoid penny stocks
                                active_pairs.append((symbol, volume))
                    except Exception as e:
                        log.debug(f"Ticker error {symbol}: {e}")
                        continue
            
            # Sort by volume
            active_pairs.sort(key=lambda x: x[1], reverse=True)
            
            # Take top N
            selected_pairs = active_pairs[:TOP_N_VOLUME]
            
            log.info(f"📊 Selected {len(selected_pairs)} pairs from OKX (Volume > ${MIN_VOLUME_USD:,.0f})")
            return selected_pairs
            
        except Exception as e:
            log.error(f"Error getting OKX pairs: {e}")
            return []
    
    async def save_signal(self, signal: ConfluenceSetup) -> bool:
        """Save signal to database"""
        try:
            # Insert signal
            await self.db.execute("""
                INSERT INTO confluence_signals (
                    id, symbol, side, entry_price, stop_loss, take_profit,
                    confluence_score, confluence_details, conditions_met,
                    expected_move, probability_score, entry_confidence, entry_type,
                    risk_pct, reward_pct, risk_reward,
                    market_structure, order_flow, momentum, liquidity_zone
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal.signal_id,
                signal.symbol,
                signal.side,
                signal.entry_price,
                signal.stop_loss,
                signal.take_profit,
                signal.confluence_score,
                json.dumps(signal.confluence_details),
                json.dumps(signal.conditions_met),
                signal.expected_move_pct,
                signal.probability_score,
                signal.entry_confidence,
                signal.entry_type,
                signal.risk_pct,
                signal.reward_pct,
                signal.risk_reward,
                json.dumps({
                    "trend": signal.market_structure.trend,
                    "aligned": signal.market_structure.higher_timeframe_aligned,
                    "key_support": signal.market_structure.key_support,
                    "key_resistance": signal.market_structure.key_resistance,
                    "score": signal.market_structure.structure_score
                }),
                json.dumps({
                    "volume_spike": signal.order_flow.volume_spike,
                    "volume_ratio": signal.order_flow.volume_ratio,
                    "imbalance": signal.order_flow.bid_ask_imbalance,
                    "score": signal.order_flow.flow_score
                }),
                json.dumps({
                    "rsi_divergence": signal.momentum.rsi_divergence,
                    "rsi_value": signal.momentum.rsi_value,
                    "macd_signal": signal.momentum.macd_signal,
                    "candle_pattern": signal.momentum.candle_pattern,
                    "score": signal.momentum.momentum_score
                }),
                json.dumps({
                    "zone_type": signal.liquidity_zone.zone_type,
                    "price_level": signal.liquidity_zone.price_level,
                    "stop_hunt_potential": signal.liquidity_zone.stop_hunt_potential,
                    "strength": signal.liquidity_zone.strength
                })
            ))
            
            await self.db.commit()
            log.info(f"✅ Confluence signal saved: {signal.symbol}")
            return True
            
        except Exception as e:
            log.error(f"Error saving signal: {e}")
            return False
    
    async def format_signal_message(self, signal: ConfluenceSetup) -> str:
        """Format confluence signal for Telegram"""
        side_emoji = "🟢" if signal.side == "LONG" else "🔴"
        side_text = "LONG" if signal.side == "LONG" else "SHORT"
        
        # Confluence score with color
        if signal.confluence_score >= 8.5:
            score_emoji = "🔥"
            score_color = "#00FF00"
        elif signal.confluence_score >= 7.5:
            score_emoji = "✅"
            score_color = "#FFFF00"
        else:
            score_emoji = "⚠️"
            score_color = "#FFA500"
        
        # Breakdown of confluence
        struct_score = signal.confluence_details["scores"]["structure"]
        flow_score = signal.confluence_details["scores"]["order_flow"]
        mom_score = signal.confluence_details["scores"]["momentum"]
        liq_score = signal.confluence_details["scores"]["liquidity"]
        
        # Key conditions
        key_conditions = []
        details = signal.confluence_details["details"]
        
        if details["structure"]["trend"] != "RANGING":
            key_conditions.append(f"Trend: {details['structure']['trend']}")
        
        if details["momentum"]["rsi_divergence"] != "NONE":
            key_conditions.append(f"RSI: {details['momentum']['rsi_divergence']}")
        
        if details["momentum"]["macd_signal"] != "NONE":
            key_conditions.append(f"MACD: {details['momentum']['macd_signal']}")
        
        if details["liquidity"]["zone_type"] != "NONE":
            key_conditions.append(f"Liquidity: {details['liquidity']['zone_type']}")
        
        conditions_text = " | ".join(key_conditions[:3])
        
        message = f"""
{side_emoji} <b>CONFLUENCE SIGNAL - {side_text}</b> {score_emoji}

<b>Exchange: OKX</b>
<b>{signal.symbol}</b>
<b>Confluence Score: <font color='{score_color}'>{signal.confluence_score:.1f}/10</font></b>

<b>📊 CONFLUENCE BREAKDOWN:</b>
‎• Market Structure: {struct_score:.1f}/2.5
‎• Order Flow: {flow_score:.1f}/3.0
‎• Momentum: {mom_score:.1f}/2.5
‎• Liquidity: {liq_score:.1f}/2.0

<b>🎯 KEY CONDITIONS:</b>
{conditions_text}

<b>⚡ ENTRY DETAILS:</b>
‎• Type: {signal.entry_type}
‎• Price: <code>{signal.entry_price:.6f}</code>
‎• Confidence: {signal.entry_confidence:.1%}

<b>🛡️ RISK MANAGEMENT:</b>
‎• Stop Loss: <code>{signal.stop_loss:.6f}</code> ({signal.risk_pct:.2f}%)
‎• Take Profit: <code>{signal.take_profit:.6f}</code> ({signal.reward_pct:.2f}%)
‎• Risk/Reward: {signal.risk_reward:.1f}:1
‎• Expected Move: {signal.expected_move_pct:.1f}%

<b>📈 PROBABILITY:</b>
‎• Hit Probability: {signal.probability_score:.1%}
‎• Conditions Met: {len(signal.conditions_met)}/4 layers

<b>⚠️ NOTE:</b>
Only one signal per symbol. New signals require better confluence or previous closure.

#Confluence{side_text} #{signal.symbol.replace('/', '')} #Expected{signal.expected_move_pct:.0f}Percent #OKX
"""
        return message
    
    async def send_telegram_alert(self, signal: ConfluenceSetup):
        """Send Telegram alert"""
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
                
            log.info(f"📤 Confluence alert sent: {signal.symbol}")
            
        except Exception as e:
            log.error(f"Telegram error: {e}")
    
    async def monitor_positions(self):
        """Monitor and close positions"""
        log.info("👀 Starting position monitoring...")
        
        while True:
            try:
                # Get open positions
                async with self.db.execute("""
                    SELECT id, symbol, side, entry_price, stop_loss, take_profit, status
                    FROM confluence_signals 
                    WHERE status IN ('PENDING', 'TRIGGERED')
                """) as cursor:
                    positions = await cursor.fetchall()
                
                if positions:
                    log.debug(f"📊 Monitoring {len(positions)} positions")
                
                for pos_id, symbol, side, entry, sl, tp, status in positions:
                    try:
                        # Get current price
                        ticker = await self.exchange.fetch_ticker(symbol)
                        current_price = ticker['last']
                        
                        # For PENDING positions: check if price reached entry
                        if status == 'PENDING':
                            if abs(current_price - entry) / entry <= 0.01:  # Within 1%
                                # Mark as triggered
                                await self.db.execute("""
                                    UPDATE confluence_signals SET 
                                        status = 'TRIGGERED',
                                        triggered_at = CURRENT_TIMESTAMP,
                                        trigger_price = ?
                                    WHERE id = ?
                                """, (current_price, pos_id))
                                
                                await self.db.commit()
                                self.scanner.signal_manager.update_signal_status(pos_id, "TRIGGERED")
                                
                                log.info(f"✅ Position triggered: {symbol} {side} @ {current_price:.4f}")
                                continue
                        
                        # Check SL/TP
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
                            # Update database
                            await self.db.execute("""
                                UPDATE confluence_signals SET 
                                    status = 'CLOSED',
                                    closed_at = CURRENT_TIMESTAMP,
                                    close_price = ?,
                                    pnl_percent = ?,
                                    close_reason = ?
                                WHERE id = ?
                            """, (current_price, pnl_percent, close_reason, pos_id))
                            
                            await self.db.commit()
                            self.scanner.signal_manager.update_signal_status(pos_id, "CLOSED")
                            
                            log.info(f"📤 Position closed: {symbol} {side} {pnl_percent:+.2f}% ({close_reason})")
                    
                    except Exception as e:
                        log.error(f"Monitor error for {symbol}: {e}")
                        continue
                
                # Clean up old signals
                self.scanner.cleanup_old_signals()
                
                await asyncio.sleep(5)  # Check every 5 seconds
                
            except Exception as e:
                log.error(f"Monitoring loop error: {e}")
                await asyncio.sleep(10)
    
    async def confluence_scanning(self):
        """Main confluence scanning loop"""
        log.info("🚀 Starting confluence scanning for 3-5% moves...")
        
        while True:
            try:
                self.scan_cycle += 1
                start_time = time.time()
                
                log.info(f"🔄 Confluence scan #{self.scan_cycle}")
                
                # Get active pairs
                pairs = await self.get_active_pairs()
                
                if not pairs:
                    log.warning("No active pairs found")
                    await asyncio.sleep(SCAN_INTERVAL)
                    continue
                
                log.info(f"Analyzing {len(pairs)} pairs for confluence")
                
                signals_found = 0
                pairs_processed = 0
                
                # Scan pairs for confluence
                for symbol, volume in pairs:
                    try:
                        # Fetch multi-timeframe data
                        multi_tf_data = await self.fetch_timeframe_data(symbol)
                        
                        # Check we have all required timeframes
                        required_tfs = ["DAILY", "4H", "1H", "15M", "5M"]
                        has_all_data = all(tf in multi_tf_data for tf in required_tfs)
                        
                        if not has_all_data:
                            log.debug(f"{symbol}: Missing timeframe data")
                            continue
                        
                        # Generate confluence signal
                        signal = self.scanner.generate_confluence_signal(multi_tf_data, symbol)
                        
                        if signal:
                            # Save and send
                            saved = await self.save_signal(signal)
                            
                            if saved:
                                await self.send_telegram_alert(signal)
                                signals_found += 1
                        
                        pairs_processed += 1
                        self.scanner.daily_stats["pairs_analyzed"] += 1
                        
                        # Small delay between pairs
                        await asyncio.sleep(0.05)
                        
                    except Exception as e:
                        log.debug(f"Pair error {symbol}: {str(e)[:50]}")
                        continue
                
                # Update scan stats
                self.scanner.daily_stats["scans"] += 1
                
                # Log stats
                stats = self.scanner.get_daily_stats()
                active_count = len(self.scanner.signal_manager.active_signals)
                
                log.info(f"📊 Confluence stats: Found {signals_found}, Active: {active_count}")
                log.info(f"   Total signals: {stats['confluence_signals']}")
                log.info(f"   High quality: {stats['high_quality_signals']}")
                log.info(f"   Rejected (low confluence): {stats['rejected_low_confluence']}")
                log.info(f"   Rejected (no alignment): {stats['rejected_no_alignment']}")
                
                scan_duration = time.time() - start_time
                log.info(f"Scan #{self.scan_cycle}: {signals_found} confluence signals in {scan_duration:.2f}s")
                
                # Wait for next scan
                wait_time = max(1.0, SCAN_INTERVAL - scan_duration)
                await asyncio.sleep(wait_time)
                
            except Exception as e:
                log.error(f"Scanning loop error: {e}")
                await asyncio.sleep(10)
    
    async def run(self):
        """Run the scanner"""
        try:
            await self.initialize()
            
            # Run both loops
            await asyncio.gather(
                self.confluence_scanning(),
                self.monitor_positions()
            )
            
        except KeyboardInterrupt:
            log.info("Confluence scanner stopped by user")
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
            active_count = len(self.scanner.signal_manager.active_signals)
            
            message = f"""
🛑 <b>CONFLUENCE SCANNER STOPPED</b>

<b>📊 FINAL STATISTICS:</b>
‎• Exchange: OKX
‎• Total scans: {stats['scans']}
‎• Pairs analyzed: {stats['pairs_analyzed']}
‎• Confluence signals: {stats['confluence_signals']}
‎• High quality (8+): {stats['high_quality_signals']}

<b>🚫 REJECTIONS:</b>
‎• Low confluence: {stats['rejected_low_confluence']}
‎• No alignment: {stats['rejected_no_alignment']}

<b>⚡ ACTIVE SIGNALS:</b>
‎• Currently active: {active_count}

<b>🎯 STRATEGY PERFORMANCE:</b>
The scanner hunted for 3-5% move setups with multi-layer confluence:
‎1. Market Structure alignment
‎2. Order flow confirmation
‎3. Momentum divergence
‎4. Liquidity zone targeting

Only signals with confluence scores ≥ {MIN_CONFLUENCE_SCORE}/10 were considered.

#ConfluenceFinalStats #MultiLayerAnalysis #OKX
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
                    "scanner": "Confluence Scanner v5.0",
                    "exchange": "OKX",
                    "target": "3-5% directional moves",
                    "scan_cycle": scanner.scan_cycle,
                    "active_signals": active_count,
                    "daily_stats": stats,
                    "strategy": {
                        "layers": ["Market Structure", "Order Flow", "Momentum", "Liquidity"],
                        "weights": CONFLUENCE_WEIGHTS,
                        "min_confluence": MIN_CONFLUENCE_SCORE,
                        "target_range": f"{TARGET_PROFIT_RANGE[0]}-{TARGET_PROFIT_RANGE[1]}%",
                        "min_rr": MIN_RISK_REWARD
                    }
                }, indent=2)
            
            elif path == '/signals':
                if scanner.db:
                    scanner.db.row_factory = aiosqlite.Row
                    async with scanner.db.execute("""
                        SELECT symbol, side, entry_price, confluence_score, 
                               expected_move, risk_reward, entry_type, status,
                               created_at, close_reason, pnl_percent
                        FROM confluence_signals 
                        ORDER BY created_at DESC 
                        LIMIT 20
                    """) as cursor:
                        rows = await cursor.fetchall()
                        signals = [dict(row) for row in rows]
                    
                    response = json.dumps({"signals": signals, "count": len(signals)}, indent=2)
                else:
                    response = json.dumps({"error": "Database not available"})
            
            elif path == '/confluence':
                response = json.dumps({
                    "exchange": "OKX",
                    "analysis_layers": {
                        "market_structure": {
                            "weight": "25%",
                            "focus": "Trend, key levels, HTF alignment",
                            "indicators": "EMA alignment, swing points, structure breaks"
                        },
                        "order_flow": {
                            "weight": "30%",
                            "focus": "Volume profile, bid/ask imbalance",
                            "indicators": "Volume spikes, orderbook depth, accumulation"
                        },
                        "momentum": {
                            "weight": "25%",
                            "focus": "Short-term momentum shifts",
                            "indicators": "RSI hidden divergence, MACD flips, candlestick patterns"
                        },
                        "liquidity": {
                            "weight": "20%",
                            "focus": "Stop hunts & liquidity zones",
                            "indicators": "Sweep highs/lows, equal highs/lows, liquidation clusters"
                        }
                    },
                    "target_parameters": {
                        "move_target": "3-5%",
                        "confluence_threshold": f"{MIN_CONFLUENCE_SCORE}/10",
                        "risk_management": f"Max SL: {MAX_STOP_LOSS}%, Min RR: {MIN_RISK_REWARD}:1"
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
    log.info(f"🌐 HTTP server started on port {port}")
    
    async with server:
        await server.serve_forever()

# ================ MAIN ================
async def main():
    """Main function"""
    scanner = ConfluenceMoveScanner()
    
    # Start HTTP server in background
    http_task = asyncio.create_task(start_confluence_server(scanner))
    
    # Run scanner
    await scanner.run()

if __name__ == "__main__":
    asyncio.run(main())