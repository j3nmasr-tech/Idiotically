#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🎯 TRADER'S CORE LOGIC SYSTEM
Professional discretionary system following human trader logic
DIRECTION FIRST → IMPULSE CONFIRMATION
TRADER MINDSET: Energy hunter, direction setter, impulse timer
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
from dataclasses import dataclass, field
import json
from enum import Enum
import traceback

# ================ CONFIGURATION ================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
DB_PATH = "/app/data/trader_core.db"

# Exchange configuration
EXCHANGE_NAME = "okx"  # Change to binance, kucoin, etc.
API_KEY = os.getenv("EXCHANGE_API_KEY", "")
API_SECRET = os.getenv("EXCHANGE_API_SECRET", "")
PASSPHRASE = os.getenv("EXCHANGE_PASSPHRASE", "")

# Asset selection
MAX_PRICE_USDT = 50.0
MIN_VOLUME_USD = 50000
MAX_VOLUME_USD = 50000000

# Timeframe configuration
TIMEFRAMES = {
    "4H": {"tf": "4h", "candles": 100, "weight": 1.3},
    "1H": {"tf": "1h", "candles": 80, "weight": 1.2},
    "15M": {"tf": "15m", "candles": 60, "weight": 1.0},
    "5M": {"tf": "5m", "candles": 40, "weight": 0.8}
}

# Trading parameters
MIN_CONFIDENCE = 0.55
MIN_RISK_REWARD = 1.8
MAX_POSITION_SIZE = 0.1  # 10% of portfolio per trade
STOP_LOSS_PCT = 0.02  # 2% initial stop loss
TAKE_PROFIT_PCT = 0.04  # 4% initial take profit
TRAILING_STOP_ACTIVATE = 0.015  # 1.5% profit activates trailing
TRAILING_STOP_DISTANCE = 0.01  # 1% trailing distance

# Wave parameters
WAVE_READY_THRESHOLD = 0.4
STRENGTH_THRESHOLD = 0.4
VOLUME_THRESHOLD = 1.3

# RSI parameters
RSI_PERIOD = 14
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30

# EMA parameters
EMA_PERIODS = [9, 21, 50]

# Scanning
SCAN_INTERVAL = 10  # seconds
MAX_SYMBOLS_PER_SCAN = 40
MAX_CONCURRENT_FETCHES = 5

# Risk management
MAX_OPEN_POSITIONS = 5
MAX_DAILY_LOSS_PCT = 3.0  # 3% max daily loss
COOLDOWN_AFTER_LOSS = 300  # 5 minutes after loss

# ================ DATA STRUCTURES ================
@dataclass
class MarketDirection:
    """Direction analysis across timeframes"""
    primary_bias: str  # STRONG_UP, UP, NEUTRAL, DOWN, STRONG_DOWN
    confidence: float
    timeframe_alignment: Dict[str, str]
    dominant_wave: str
    alignment_score: float
    
    @property
    def should_trade(self) -> bool:
        return self.primary_bias in ["STRONG_UP", "UP", "DOWN", "STRONG_DOWN"] and self.confidence >= MIN_CONFIDENCE
    
    @property
    def is_long(self) -> bool:
        return self.primary_bias in ["STRONG_UP", "UP"]
    
    @property
    def is_short(self) -> bool:
        return self.primary_bias in ["STRONG_DOWN", "DOWN"]

@dataclass
class WaveEnergy:
    """Wave length and energy analysis"""
    wave_type: str
    wave_stage: str
    wave_length_score: float
    energy_level: float
    compression_ratio: float
    momentum_gradient: float
    volume_profile: float
    candle_consistency: float
    
    @property
    def ready_for_move(self) -> bool:
        return all([
            self.energy_level >= WAVE_READY_THRESHOLD,
            self.compression_ratio >= 0.3,
            self.momentum_gradient > -0.5
        ])

@dataclass
class StrengthVolume:
    """Strength and volume analysis"""
    strength_score: float
    volume_score: float
    market_participation: float
    body_dominance: float
    volume_expansion: float
    bid_ask_balance: float
    
    @property
    def is_confirmed(self) -> bool:
        return all([
            self.strength_score >= STRENGTH_THRESHOLD,
            self.volume_score >= VOLUME_THRESHOLD / 3,
            self.body_dominance >= 0.4
        ])

@dataclass
class IndicatorAlignment:
    """Indicator analysis"""
    rsi_position: float
    rsi_momentum: float
    ema_alignment: float
    volume_trend: float
    ema_order: str
    price_vs_ema: str
    macd_signal: str
    
    @property
    def supports_long(self) -> bool:
        return all([
            self.rsi_position <= RSI_OVERBOUGHT - 10,
            self.rsi_momentum >= -0.3,
            self.ema_order in ["BULLISH", "MIXED"],
            self.macd_signal in ["BULLISH", "NEUTRAL"]
        ])
    
    @property
    def supports_short(self) -> bool:
        return all([
            self.rsi_position >= RSI_OVERSOLD + 10,
            self.rsi_momentum <= 0.3,
            self.ema_order in ["BEARISH", "MIXED"],
            self.macd_signal in ["BEARISH", "NEUTRAL"]
        ])

@dataclass
class TradeSignal:
    """Complete trade signal"""
    signal_id: str
    symbol: str
    direction: str
    timestamp: float
    
    # Analysis components
    market_direction: MarketDirection
    wave_energy: WaveEnergy
    strength_volume: StrengthVolume
    indicators: IndicatorAlignment
    
    # Trade parameters
    entry_price: float
    stop_loss: float
    take_profit: float
    wave_target: float
    
    # Metrics
    overall_confidence: float
    timeframe_alignment: float
    risk_reward: float
    quality_score: float
    
    # Context
    primary_timeframe: str
    entry_timeframe: str
    
    # Status
    status: str = "PENDING"  # PENDING, TRIGGERED, CLOSED
    entry_time: Optional[float] = None
    exit_time: Optional[float] = None
    exit_price: Optional[float] = None
    pnl_pct: Optional[float] = None
    exit_reason: Optional[str] = None

@dataclass
class Position:
    """Active position"""
    signal: TradeSignal
    size: float
    entry_time: float
    current_stop: float
    highest_price: float = 0
    lowest_price: float = float('inf')
    trailing_active: bool = False

@dataclass
class PerformanceStats:
    """Performance tracking"""
    total_scans: int = 0
    signals_found: int = 0
    trades_executed: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    total_pnl_pct: float = 0.0
    max_consecutive_wins: int = 0
    max_consecutive_losses: int = 0
    current_streak: int = 0
    is_winning_streak: bool = True
    daily_pnl_pct: float = 0.0
    daily_loss_pct: float = 0.0

# ================ LOGGING ================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)8s | %(name)s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger("trader_core")

# ================ CORE ENGINE ================
class TraderCoreEngine:
    """Core trading engine implementing trader's logic"""
    
    def __init__(self):
        self.positions: Dict[str, Position] = {}
        self.signals: Dict[str, TradeSignal] = {}
        self.stats = PerformanceStats()
        self.daily_start_time = time.time()
        self.last_loss_time = 0
        self.exchange = None
        self.db = None
        
    async def initialize(self):
        """Initialize engine"""
        await self._init_database()
        await self._init_exchange()
        
    async def _init_database(self):
        """Initialize database"""
        try:
            os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
            self.db = await aiosqlite.connect(DB_PATH)
            
            # Create tables
            await self.db.execute("""
                CREATE TABLE IF NOT EXISTS signals (
                    id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    status TEXT NOT NULL,
                    
                    entry_price REAL,
                    stop_loss REAL,
                    take_profit REAL,
                    wave_target REAL,
                    
                    confidence REAL,
                    risk_reward REAL,
                    quality_score REAL,
                    
                    primary_tf TEXT,
                    entry_tf TEXT,
                    
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    triggered_at TIMESTAMP,
                    closed_at TIMESTAMP,
                    
                    exit_price REAL,
                    pnl_pct REAL,
                    exit_reason TEXT
                )
            """)
            
            await self.db.execute("""
                CREATE TABLE IF NOT EXISTS performance (
                    date DATE PRIMARY KEY,
                    signals_found INTEGER,
                    trades_executed INTEGER,
                    winning_trades INTEGER,
                    losing_trades INTEGER,
                    total_pnl_pct REAL,
                    win_rate REAL,
                    avg_pnl REAL,
                    best_trade REAL,
                    worst_trade REAL
                )
            """)
            
            await self.db.commit()
            log.info("✅ Database initialized")
            
        except Exception as e:
            log.error(f"Database init error: {e}")
            raise
    
    async def _init_exchange(self):
        """Initialize exchange connection"""
        try:
            exchange_class = getattr(ccxt, EXCHANGE_NAME)
            config = {
                "enableRateLimit": True,
                "options": {"defaultType": "spot"},
                "timeout": 30000,
            }
            
            if API_KEY and API_SECRET:
                config.update({
                    "apiKey": API_KEY,
                    "secret": API_SECRET,
                })
                if PASSPHRASE:
                    config["password"] = PASSPHRASE
            
            self.exchange = exchange_class(config)
            
            # Test connection
            markets = await self.exchange.load_markets()
            log.info(f"✅ Exchange {EXCHANGE_NAME} connected. {len(markets)} markets loaded")
            
        except Exception as e:
            log.error(f"Exchange init error: {e}")
            raise
    
    # ========== ASSET MANAGEMENT ==========
    
    async def get_trading_symbols(self) -> List[str]:
        """Get symbols for trading"""
        try:
            markets = await self.exchange.load_markets()
            symbols = []
            
            for symbol, market in markets.items():
                if not market.get('active', True):
                    continue
                
                # Filter for USDT pairs
                if not symbol.endswith('/USDT'):
                    continue
                
                # Check if spot trading is available
                if not market.get('spot', False):
                    continue
                
                symbols.append(symbol)
            
            # Prioritize by volume
            prioritized = await self._prioritize_symbols(symbols[:100])
            return prioritized[:MAX_SYMBOLS_PER_SCAN]
            
        except Exception as e:
            log.error(f"Error getting symbols: {e}")
            return ["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT", "XRP/USDT"]
    
    async def _prioritize_symbols(self, symbols: List[str]) -> List[str]:
        """Prioritize symbols by opportunity"""
        prioritized = []
        
        for symbol in symbols:
            try:
                ticker = await self.exchange.fetch_ticker(symbol)
                price = ticker['last']
                volume = ticker.get('quoteVolume', 0)
                
                # Basic filters
                if price > MAX_PRICE_USDT:
                    continue
                if volume < MIN_VOLUME_USD or volume > MAX_VOLUME_USD:
                    continue
                
                prioritized.append(symbol)
                
            except Exception as e:
                continue
        
        return prioritized
    
    # ========== DATA FETCHING ==========
    
    async def fetch_multi_tf_data(self, symbol: str) -> Dict[str, pd.DataFrame]:
        """Fetch data for all timeframes"""
        data = {}
        
        tasks = []
        for tf_name, tf_config in TIMEFRAMES.items():
            task = self._fetch_single_tf(symbol, tf_name, tf_config)
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for tf_name, result in zip(TIMEFRAMES.keys(), results):
            if isinstance(result, pd.DataFrame) and not result.empty:
                data[tf_name] = result
        
        return data
    
    async def _fetch_single_tf(self, symbol: str, tf_name: str, config: Dict) -> Optional[pd.DataFrame]:
        """Fetch data for single timeframe"""
        try:
            ohlcv = await self.exchange.fetch_ohlcv(
                symbol, 
                timeframe=config['tf'], 
                limit=config['candles']
            )
            
            if not ohlcv or len(ohlcv) < 20:
                return None
            
            df = pd.DataFrame(
                ohlcv,
                columns=["timestamp", "open", "high", "low", "close", "volume"]
            )
            
            # Convert types
            for col in ["open", "high", "low", "close", "volume"]:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            df = df.dropna()
            
            if len(df) >= 20:
                # Add technical indicators
                df = self._add_indicators(df)
                return df
            
        except Exception as e:
            log.debug(f"Fetch error {symbol} {tf_name}: {str(e)[:50]}")
        
        return None
    
    def _add_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add technical indicators to dataframe"""
        # RSI
        df['rsi'] = self._calculate_rsi(df['close'])
        
        # EMAs
        for period in EMA_PERIODS:
            df[f'ema_{period}'] = df['close'].ewm(span=period, adjust=False).mean()
        
        # MACD
        macd, signal, hist = self._calculate_macd(df['close'])
        df['macd'] = macd
        df['macd_signal'] = signal
        df['macd_hist'] = hist
        
        # Volume indicators
        df['volume_sma'] = df['volume'].rolling(window=20).mean()
        df['volume_ratio'] = df['volume'] / df['volume_sma']
        
        return df
    
    def _calculate_rsi(self, prices: pd.Series, period: int = RSI_PERIOD) -> pd.Series:
        """Calculate RSI"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def _calculate_macd(self, prices: pd.Series, 
                       fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """Calculate MACD"""
        exp1 = prices.ewm(span=fast, adjust=False).mean()
        exp2 = prices.ewm(span=slow, adjust=False).mean()
        macd = exp1 - exp2
        signal_line = macd.ewm(span=signal, adjust=False).mean()
        histogram = macd - signal_line
        return macd, signal_line, histogram
    
    # ========== STEP 1: WATCH ALL TIMEFRAMES ==========
    
    async def analyze_market_direction(self, multi_tf_data: Dict[str, pd.DataFrame]) -> MarketDirection:
        """Analyze direction across all timeframes"""
        
        if not multi_tf_data:
            return MarketDirection("NEUTRAL", 0.0, {}, "UNKNOWN", 0.0)
        
        timeframe_biases = {}
        biases = []
        confidences = []
        weights = []
        
        for tf_name, df in multi_tf_data.items():
            if df is None or len(df) < 20:
                continue
            
            bias, confidence = self._analyze_single_tf_direction(df, tf_name)
            timeframe_biases[tf_name] = bias
            biases.append(bias)
            confidences.append(confidence)
            weights.append(TIMEFRAMES[tf_name]['weight'])
        
        if not biases:
            return MarketDirection("NEUTRAL", 0.0, {}, "UNKNOWN", 0.0)
        
        # Weighted aggregation
        primary_bias = self._aggregate_weighted_biases(biases, confidences, weights)
        overall_confidence = np.average(confidences, weights=weights)
        
        # Calculate alignment
        alignment_score = self._calculate_alignment_score(timeframe_biases, primary_bias)
        
        # Detect dominant wave
        dominant_wave = self._detect_dominant_wave(multi_tf_data)
        
        return MarketDirection(
            primary_bias=primary_bias,
            confidence=float(overall_confidence),
            timeframe_alignment=timeframe_biases,
            dominant_wave=dominant_wave,
            alignment_score=alignment_score
        )
    
    def _analyze_single_tf_direction(self, df: pd.DataFrame, tf_name: str) -> Tuple[str, float]:
        """Analyze direction for single timeframe"""
        try:
            prices = df['close'].values[-20:]
            highs = df['high'].values[-20:]
            lows = df['low'].values[-20:]
            volumes = df['volume'].values[-20:]
            
            # Price action metrics
            x = np.arange(len(prices))
            slope, _ = np.polyfit(x, prices, 1)
            slope_pct = (slope / prices[0]) * 100 if prices[0] != 0 else 0
            
            # Structure analysis
            higher_highs = sum(1 for i in range(1, len(highs)) if highs[i] > highs[i-1])
            higher_lows = sum(1 for i in range(1, len(lows)) if lows[i] > lows[i-1])
            lower_highs = sum(1 for i in range(1, len(highs)) if highs[i] < highs[i-1])
            lower_lows = sum(1 for i in range(1, len(lows)) if lows[i] < lows[i-1])
            
            # Volume analysis
            volume_up = 0
            volume_down = 0
            for i in range(1, len(prices)):
                if prices[i] > prices[i-1]:
                    volume_up += volumes[i]
                elif prices[i] < prices[i-1]:
                    volume_down += volumes[i]
            
            total_volume = volume_up + volume_down
            volume_ratio = volume_up / total_volume if total_volume > 0 else 0.5
            
            # Calculate scores
            up_score = (higher_highs + higher_lows) * 0.3 + volume_ratio * 0.4
            down_score = (lower_highs + lower_lows) * 0.3 + (1 - volume_ratio) * 0.4
            
            # Determine bias
            if abs(slope_pct) > 2.0:
                if slope_pct > 0:
                    bias = "STRONG_UP"
                    confidence = min(abs(slope_pct) / 5.0, 1.0)
                else:
                    bias = "STRONG_DOWN"
                    confidence = min(abs(slope_pct) / 5.0, 1.0)
            elif up_score > down_score * 1.5:
                bias = "UP"
                confidence = up_score / (up_score + down_score)
            elif down_score > up_score * 1.5:
                bias = "DOWN"
                confidence = down_score / (up_score + down_score)
            else:
                bias = "NEUTRAL"
                confidence = 0.3
            
            return bias, min(confidence, 1.0)
            
        except Exception as e:
            return "NEUTRAL", 0.2
    
    def _aggregate_weighted_biases(self, biases: List[str], confidences: List[float], 
                                  weights: List[float]) -> str:
        """Aggregate biases with weighting"""
        bias_scores = {
            "STRONG_UP": 0.0,
            "UP": 0.0,
            "NEUTRAL": 0.0,
            "DOWN": 0.0,
            "STRONG_DOWN": 0.0
        }
        
        for bias, confidence, weight in zip(biases, confidences, weights):
            if bias in bias_scores:
                bias_scores[bias] += confidence * weight
        
        # Find strongest bias
        strongest = max(bias_scores.items(), key=lambda x: x[1])
        
        # Apply threshold
        if strongest[1] < 1.0:
            return "NEUTRAL"
        
        return strongest[0]
    
    def _calculate_alignment_score(self, timeframe_biases: Dict[str, str], 
                                  primary_bias: str) -> float:
        """Calculate alignment score across timeframes"""
        if not timeframe_biases:
            return 0.0
        
        aligned = 0
        total = 0
        
        bias_groups = {
            "STRONG_UP": ["STRONG_UP", "UP"],
            "UP": ["STRONG_UP", "UP", "NEUTRAL"],
            "DOWN": ["STRONG_DOWN", "DOWN", "NEUTRAL"],
            "STRONG_DOWN": ["STRONG_DOWN", "DOWN"]
        }
        
        allowed_biases = bias_groups.get(primary_bias, [])
        
        for bias in timeframe_biases.values():
            total += 1
            if bias in allowed_biases:
                aligned += 1
        
        return aligned / total if total > 0 else 0.0
    
    def _detect_dominant_wave(self, multi_tf_data: Dict[str, pd.DataFrame]) -> str:
        """Detect dominant wave type"""
        try:
            # Use 1H or 4H for wave detection
            tf_to_check = multi_tf_data.get("4H") or multi_tf_data.get("1H")
            if tf_to_check is None or len(tf_to_check) < 30:
                return "UNKNOWN"
            
            prices = tf_to_check['close'].values[-30:]
            ranges = tf_to_check['high'].values[-30:] - tf_to_check['low'].values[-30:]
            
            # Compression detection
            range_cv = np.std(ranges[-10:]) / np.mean(ranges[-10:]) if np.mean(ranges[-10:]) > 0 else 1
            if range_cv < 0.3:
                return "COMPRESSION"
            
            # Trend strength
            price_change = abs(prices[-1] - prices[0]) / prices[0] * 100
            if price_change > 4:
                # Check if impulse or correction
                rsi = tf_to_check['rsi'].values[-10:]
                if (price_change > 0 and np.mean(rsi) > 60) or (price_change < 0 and np.mean(rsi) < 40):
                    return "IMPULSE"
                else:
                    return "CORRECTION"
            
            return "NEUTRAL"
            
        except Exception as e:
            return "UNKNOWN"
    
    # ========== STEP 2: WAVE ENERGY ANALYSIS ==========
    
    def analyze_wave_energy(self, df: pd.DataFrame, direction: str) -> WaveEnergy:
        """Analyze wave energy and length"""
        
        try:
            if df is None or len(df) < 30:
                return self._default_wave_energy()
            
            # Determine wave type and stage
            wave_type, wave_stage = self._identify_wave_type_stage(df, direction)
            
            # Calculate metrics
            wave_length_score = self._calculate_wave_length_score(df, direction)
            energy_level = self._calculate_energy_level(df)
            compression_ratio = self._calculate_compression_ratio(df)
            momentum_gradient = self._calculate_momentum_gradient(df)
            volume_profile = self._calculate_volume_profile(df)
            candle_consistency = self._calculate_candle_consistency(df, direction)
            
            return WaveEnergy(
                wave_type=wave_type,
                wave_stage=wave_stage,
                wave_length_score=float(wave_length_score),
                energy_level=float(energy_level),
                compression_ratio=float(compression_ratio),
                momentum_gradient=float(momentum_gradient),
                volume_profile=float(volume_profile),
                candle_consistency=float(candle_consistency)
            )
            
        except Exception as e:
            log.error(f"Wave energy error: {e}")
            return self._default_wave_energy()
    
    def _identify_wave_type_stage(self, df: pd.DataFrame, direction: str) -> Tuple[str, str]:
        """Identify wave type and stage"""
        try:
            prices = df['close'].values[-20:]
            
            # Trend calculation
            x = np.arange(len(prices))
            slope, _ = np.polyfit(x, prices, 1)
            trend_strength = abs(slope / prices[0]) * 100 if prices[0] != 0 else 0
            
            # Wave type
            if trend_strength < 0.5:
                wave_type = "COMPRESSION"
            elif (direction == "LONG" and slope > 0) or (direction == "SHORT" and slope < 0):
                wave_type = "IMPULSE"
            else:
                wave_type = "CORRECTION"
            
            # Wave stage
            if wave_type == "COMPRESSION":
                stage = "MATURE" if len(prices) > 15 else "DEVELOPING"
            else:
                price_change = abs(prices[-1] - prices[0]) / prices[0] * 100
                if price_change < 2:
                    stage = "EARLY"
                elif price_change < 5:
                    stage = "MID"
                else:
                    stage = "LATE"
            
            return wave_type, stage
            
        except Exception as e:
            return "UNKNOWN", "UNKNOWN"
    
    def _calculate_wave_length_score(self, df: pd.DataFrame, direction: str) -> float:
        """Calculate wave length potential"""
        try:
            # Look at volatility and recent moves
            prices = df['close'].values[-30:]
            returns = np.diff(prices) / prices[:-1]
            
            # Calculate expected move based on volatility
            volatility = np.std(returns) * np.sqrt(252)  # Annualized
            
            # Adjust for compression
            ranges = df['high'].values[-10:] - df['low'].values[-10:]
            range_ratio = np.mean(ranges) / np.std(ranges) if np.std(ranges) > 0 else 1
            
            score = min(volatility * 2 * range_ratio, 1.0)
            return max(score, 0.1)
            
        except Exception as e:
            return 0.5
    
    def _calculate_energy_level(self, df: pd.DataFrame) -> float:
        """Calculate accumulated energy"""
        try:
            # Energy = compression + volume accumulation
            ranges = df['high'] - df['low']
            volumes = df['volume']
            
            # Compression component
            recent_range = ranges.iloc[-5:].mean()
            avg_range = ranges.iloc[:-5].mean() if len(ranges) > 5 else recent_range
            compression = 1.0 - (recent_range / avg_range if avg_range > 0 else 0.5)
            
            # Volume accumulation
            recent_volume = volumes.iloc[-5:].mean()
            avg_volume = volumes.iloc[:-5].mean() if len(volumes) > 5 else recent_volume
            volume_ratio = min(recent_volume / avg_volume if avg_volume > 0 else 1.0, 2.0)
            
            energy = (compression * 0.6 + (volume_ratio / 2) * 0.4)
            return min(energy, 1.0)
            
        except Exception as e:
            return 0.5
    
    def _calculate_compression_ratio(self, df: pd.DataFrame) -> float:
        """Calculate compression ratio"""
        try:
            ranges = df['high'] - df['low']
            if len(ranges) < 10:
                return 0.5
            
            recent_avg = ranges.iloc[-5:].mean()
            historical_avg = ranges.iloc[:-5].mean() if len(ranges) > 5 else recent_avg
            
            if historical_avg > 0:
                ratio = recent_avg / historical_avg
                return max(0.0, 1.0 - ratio)
            
            return 0.5
            
        except Exception as e:
            return 0.5
    
    def _calculate_momentum_gradient(self, df: pd.DataFrame) -> float:
        """Calculate momentum gradient"""
        try:
            if len(df) < 10:
                return 0.0
            
            prices = df['close'].values
            half = len(prices) // 2
            
            def calc_momentum(data):
                if len(data) < 3:
                    return 0.0
                returns = np.diff(data) / data[:-1]
                return np.mean(returns) * 100
            
            mom1 = calc_momentum(prices[:half])
            mom2 = calc_momentum(prices[half:])
            
            if abs(mom1) > 0:
                gradient = (mom2 - mom1) / abs(mom1)
                return np.clip(gradient, -1, 1)
            
            return 0.0
            
        except Exception as e:
            return 0.0
    
    def _calculate_volume_profile(self, df: pd.DataFrame) -> float:
        """Calculate volume profile quality"""
        try:
            volumes = df['volume'].values
            prices = df['close'].values
            
            if len(prices) < 10:
                return 0.5
            
            # Calculate volume distribution
            price_changes = np.diff(prices) / prices[:-1]
            up_volume = volumes[1:][price_changes > 0].sum()
            down_volume = volumes[1:][price_changes < 0].sum()
            
            total = up_volume + down_volume
            if total > 0:
                imbalance = abs(up_volume - down_volume) / total
                return 1.0 - imbalance
            
            return 0.5
            
        except Exception as e:
            return 0.5
    
    def _calculate_candle_consistency(self, df: pd.DataFrame, direction: str) -> float:
        """Calculate candle consistency"""
        try:
            if len(df) < 5:
                return 0.5
            
            closes = df['close'].values
            opens = df['open'].values
            
            consistent = 0
            total = len(closes) - 1
            
            for i in range(1, len(closes)):
                if direction == "LONG":
                    # Prefer bullish or small bearish candles
                    is_bullish = closes[i] > opens[i]
                    is_small_bearish = closes[i] < opens[i] and (opens[i] - closes[i]) / opens[i] < 0.005
                    if is_bullish or is_small_bearish:
                        consistent += 1
                else:
                    # Prefer bearish or small bullish candles
                    is_bearish = closes[i] < opens[i]
                    is_small_bullish = closes[i] > opens[i] and (closes[i] - opens[i]) / opens[i] < 0.005
                    if is_bearish or is_small_bullish:
                        consistent += 1
            
            return consistent / total if total > 0 else 0.5
            
        except Exception as e:
            return 0.5
    
    # ========== STEP 3: STRENGTH + VOLUME ==========
    
    def analyze_strength_volume(self, df: pd.DataFrame, direction: str) -> StrengthVolume:
        """Analyze strength and volume"""
        
        try:
            if df is None or len(df) < 10:
                return self._default_strength_volume()
            
            # Calculate metrics
            strength_score = self._calculate_strength_score(df, direction)
            volume_score = self._calculate_volume_score(df)
            market_participation = self._calculate_market_participation(df, direction)
            body_dominance = self._calculate_body_dominance(df)
            volume_expansion = self._calculate_volume_expansion(df)
            bid_ask_balance = self._calculate_bid_ask_balance(df, direction)
            
            return StrengthVolume(
                strength_score=float(strength_score),
                volume_score=float(volume_score),
                market_participation=float(market_participation),
                body_dominance=float(body_dominance),
                volume_expansion=float(volume_expansion),
                bid_ask_balance=float(bid_ask_balance)
            )
            
        except Exception as e:
            log.error(f"Strength volume error: {e}")
            return self._default_strength_volume()
    
    def _calculate_strength_score(self, df: pd.DataFrame, direction: str) -> float:
        """Calculate move strength"""
        try:
            if len(df) < 3:
                return 0.0
            
            # Analyze recent candles
            recent = df.iloc[-3:]
            scores = []
            
            for _, candle in recent.iterrows():
                body = abs(candle['close'] - candle['open'])
                range_ = candle['high'] - candle['low']
                
                if range_ > 0:
                    body_ratio = body / range_
                    
                    # Direction-specific scoring
                    if direction == "LONG":
                        if candle['close'] > candle['open']:
                            scores.append(body_ratio * 1.5)  # Bullish
                        else:
                            scores.append(body_ratio * 0.7)  # Bearish in long setup
                    else:
                        if candle['close'] < candle['open']:
                            scores.append(body_ratio * 1.5)  # Bearish
                        else:
                            scores.append(body_ratio * 0.7)  # Bullish in short setup
            
            if scores:
                return min(np.mean(scores) * 1.5, 1.0)
            return 0.0
            
        except Exception as e:
            return 0.0
    
    def _calculate_volume_score(self, df: pd.DataFrame) -> float:
        """Calculate volume confirmation score"""
        try:
            if len(df) < 5:
                return 0.5
            
            volumes = df['volume'].values
            recent_volume = volumes[-3:].mean()
            avg_volume = volumes[:-3].mean() if len(volumes) > 3 else recent_volume
            
            if avg_volume > 0:
                ratio = recent_volume / avg_volume
                return min(ratio / 2.0, 1.0)  # Cap at 2x
            
            return 0.5
            
        except Exception as e:
            return 0.5
    
    def _calculate_market_participation(self, df: pd.DataFrame, direction: str) -> float:
        """Calculate market participation"""
        try:
            volumes = df['volume'].values[-10:]
            closes = df['close'].values[-10:]
            
            if len(closes) < 3:
                return 0.5
            
            # Count significant moves with volume
            significant_moves = 0
            for i in range(1, len(closes)):
                change_pct = abs(closes[i] - closes[i-1]) / closes[i-1] * 100
                volume_ratio = volumes[i] / np.mean(volumes[:i]) if i > 1 else 1.0
                
                if change_pct > 0.3 and volume_ratio > 1.2:
                    significant_moves += 1
            
            participation = significant_moves / (len(closes) - 1)
            return min(participation * 1.5, 1.0)
            
        except Exception as e:
            return 0.5
    
    def _calculate_body_dominance(self, df: pd.DataFrame) -> float:
        """Calculate body dominance"""
        try:
            bodies = abs(df['close'] - df['open'])
            ranges = df['high'] - df['low']
            
            valid = ranges > 0
            if valid.any():
                ratios = bodies[valid] / ranges[valid]
                return float(ratios.mean())
            
            return 0.5
            
        except Exception as e:
            return 0.5
    
    def _calculate_volume_expansion(self, df: pd.DataFrame) -> float:
        """Calculate volume expansion"""
        try:
            volumes = df['volume'].values
            if len(volumes) < 5:
                return 1.0
            
            recent = volumes[-2:].mean()
            previous = volumes[-4:-2].mean() if len(volumes) > 4 else recent
            
            if previous > 0:
                return recent / previous
            
            return 1.0
            
        except Exception as e:
            return 1.0
    
    def _calculate_bid_ask_balance(self, df: pd.DataFrame, direction: str) -> float:
        """Estimate bid-ask balance"""
        try:
            closes = df['close'].values[-5:]
            opens = df['open'].values[-5:]
            
            bullish = sum(1 for i in range(len(closes)) if closes[i] > opens[i])
            bearish = sum(1 for i in range(len(closes)) if closes[i] < opens[i])
            total = bullish + bearish
            
            if total > 0:
                balance = (bullish - bearish) / total
                
                # Direction adjustment
                if direction == "LONG":
                    return max(balance, -0.3)
                else:
                    return min(balance, 0.3)
            
            return 0.0
            
        except Exception as e:
            return 0.0
    
    # ========== STEP 4: INDICATOR ANALYSIS ==========
    
    def analyze_indicators(self, df: pd.DataFrame, direction: str) -> IndicatorAlignment:
        """Analyze indicators"""
        
        try:
            if df is None or len(df) < 30:
                return self._default_indicators()
            
            # RSI analysis
            rsi_position, rsi_momentum = self._analyze_rsi(df)
            
            # EMA analysis
            ema_alignment, ema_order, price_vs_ema = self._analyze_emas(df)
            
            # Volume trend
            volume_trend = self._analyze_volume_trend(df)
            
            # MACD signal
            macd_signal = self._analyze_macd(df, direction)
            
            return IndicatorAlignment(
                rsi_position=float(rsi_position),
                rsi_momentum=float(rsi_momentum),
                ema_alignment=float(ema_alignment),
                volume_trend=float(volume_trend),
                ema_order=ema_order,
                price_vs_ema=price_vs_ema,
                macd_signal=macd_signal
            )
            
        except Exception as e:
            log.error(f"Indicator error: {e}")
            return self._default_indicators()
    
    def _analyze_rsi(self, df: pd.DataFrame) -> Tuple[float, float]:
        """Analyze RSI"""
        try:
            if 'rsi' not in df.columns or len(df) < 10:
                return 50.0, 0.0
            
            rsi = df['rsi'].values[-10:]
            current_rsi = rsi[-1]
            
            # Momentum calculation
            if len(rsi) >= 5:
                x = np.arange(len(rsi))
                slope, _ = np.polyfit(x, rsi, 1)
                momentum = slope / 10  # Normalized
            else:
                momentum = 0.0
            
            return float(current_rsi), float(momentum)
            
        except Exception as e:
            return 50.0, 0.0
    
    def _analyze_emas(self, df: pd.DataFrame) -> Tuple[float, str, str]:
        """Analyze EMA alignment"""
        try:
            price = df['close'].iloc[-1]
            
            # Get EMAs
            ema_fast = df[f'ema_{EMA_PERIODS[0]}'].iloc[-1]
            ema_medium = df[f'ema_{EMA_PERIODS[1]}'].iloc[-1]
            ema_slow = df[f'ema_{EMA_PERIODS[2]}'].iloc[-1]
            
            # Determine order
            if ema_fast > ema_medium > ema_slow:
                order = "BULLISH"
            elif ema_fast < ema_medium < ema_slow:
                order = "BEARISH"
            else:
                order = "MIXED"
            
            # Price position
            if price > ema_fast and price > ema_medium and price > ema_slow:
                price_position = "ABOVE_ALL"
            elif price < ema_fast and price < ema_medium and price < ema_slow:
                price_position = "BELOW_ALL"
            else:
                price_position = "BETWEEN"
            
            # Alignment score
            distances = [
                abs(ema_fast - ema_medium) / ema_medium if ema_medium > 0 else 0,
                abs(ema_medium - ema_slow) / ema_slow if ema_slow > 0 else 0
            ]
            alignment = 1.0 - np.mean(distances) * 20  # Convert to 0-1
            
            return float(np.clip(alignment, 0.0, 1.0)), order, price_position
            
        except Exception as e:
            return 0.5, "MIXED", "BETWEEN"
    
    def _analyze_volume_trend(self, df: pd.DataFrame) -> float:
        """Analyze volume trend"""
        try:
            if 'volume_ratio' not in df.columns or len(df) < 10:
                return 0.0
            
            ratios = df['volume_ratio'].values[-10:]
            if len(ratios) >= 5:
                x = np.arange(len(ratios))
                slope, _ = np.polyfit(x, ratios, 1)
                return float(np.clip(slope, -1, 1))
            
            return 0.0
            
        except Exception as e:
            return 0.0
    
    def _analyze_macd(self, df: pd.DataFrame, direction: str) -> str:
        """Analyze MACD"""
        try:
            if 'macd' not in df.columns or 'macd_signal' not in df.columns:
                return "NEUTRAL"
            
            macd = df['macd'].iloc[-1]
            signal = df['macd_signal'].iloc[-1]
            hist = df['macd_hist'].iloc[-1]
            
            if macd > signal and hist > 0:
                return "BULLISH"
            elif macd < signal and hist < 0:
                return "BEARISH"
            else:
                return "NEUTRAL"
                
        except Exception as e:
            return "NEUTRAL"
    
    # ========== STEP 5: SIGNAL GENERATION ==========
    
    async def generate_trade_signal(self, symbol: str, 
                                   multi_tf_data: Dict[str, pd.DataFrame]) -> Optional[TradeSignal]:
        """Generate complete trade signal"""
        
        try:
            # Update stats
            self.stats.total_scans += 1
            
            # 1. WATCH ALL TIMEFRAMES - Determine direction
            market_direction = await self.analyze_market_direction(multi_tf_data)
            
            if not market_direction.should_trade:
                return None
            
            # Determine direction
            if market_direction.is_long:
                direction = "LONG"
            elif market_direction.is_short:
                direction = "SHORT"
            else:
                return None
            
            # Get entry timeframe data (15M preferred)
            entry_tf_data = multi_tf_data.get("15M") or multi_tf_data.get("5M")
            if entry_tf_data is None or len(entry_tf_data) < 20:
                return None
            
            # 2. WAVE ENERGY - Estimate potential
            wave_energy = self.analyze_wave_energy(entry_tf_data, direction)
            
            if not wave_energy.ready_for_move:
                return None
            
            # 3. STRENGTH + VOLUME - Confirm move
            strength_volume = self.analyze_strength_volume(entry_tf_data, direction)
            
            if not strength_volume.is_confirmed:
                return None
            
            # 4. INDICATORS - Fine-tune timing
            indicators = self.analyze_indicators(entry_tf_data, direction)
            
            if direction == "LONG" and not indicators.supports_long:
                return None
            elif direction == "SHORT" and not indicators.supports_short:
                return None
            
            # 5. CALCULATE TRADE PARAMETERS
            current_price = entry_tf_data['close'].iloc[-1]
            
            if direction == "LONG":
                stop_loss = current_price * (1 - STOP_LOSS_PCT)
                take_profit = current_price * (1 + TAKE_PROFIT_PCT)
                wave_target = current_price * (1 + wave_energy.wave_length_score * 0.1)
            else:
                stop_loss = current_price * (1 + STOP_LOSS_PCT)
                take_profit = current_price * (1 - TAKE_PROFIT_PCT)
                wave_target = current_price * (1 - wave_energy.wave_length_score * 0.1)
            
            # Calculate risk:reward
            risk = abs(current_price - stop_loss) / current_price
            reward = abs(take_profit - current_price) / current_price
            risk_reward = reward / risk if risk > 0 else 0
            
            if risk_reward < MIN_RISK_REWARD:
                return None
            
            # Calculate confidence scores
            confidence_scores = [
                market_direction.confidence,
                wave_energy.energy_level,
                strength_volume.strength_score,
                indicators.ema_alignment,
                market_direction.alignment_score
            ]
            
            overall_confidence = float(np.mean(confidence_scores))
            
            if overall_confidence < MIN_CONFIDENCE:
                return None
            
            # Calculate quality score
            quality_components = [
                overall_confidence * 0.3,
                (risk_reward / 3) * 0.2,  # Normalize RR
                wave_energy.compression_ratio * 0.2,
                strength_volume.volume_score * 0.15,
                indicators.ema_alignment * 0.15
            ]
            
            quality_score = float(np.sum(quality_components))
            
            # Create signal
            signal_id = hashlib.md5(
                f"{symbol}:{direction}:{current_price}:{time.time()}".encode()
            ).hexdigest()[:16]
            
            signal = TradeSignal(
                signal_id=signal_id,
                symbol=symbol,
                direction=direction,
                timestamp=time.time(),
                
                market_direction=market_direction,
                wave_energy=wave_energy,
                strength_volume=strength_volume,
                indicators=indicators,
                
                entry_price=float(current_price),
                stop_loss=float(stop_loss),
                take_profit=float(take_profit),
                wave_target=float(wave_target),
                
                overall_confidence=overall_confidence,
                timeframe_alignment=market_direction.alignment_score,
                risk_reward=float(risk_reward),
                quality_score=quality_score,
                
                primary_timeframe="4H" if "4H" in multi_tf_data else "1H",
                entry_timeframe="15M" if "15M" in multi_tf_data else "5M"
            )
            
            # Check risk management
            if not self._check_risk_management(signal):
                return None
            
            # Update stats
            self.stats.signals_found += 1
            
            log.info(f"🎯 Signal: {symbol} {direction} "
                    f"(Conf: {overall_confidence:.1%}, RR: {risk_reward:.2f}, "
                    f"Quality: {quality_score:.1%})")
            
            return signal
            
        except Exception as e:
            log.error(f"Signal generation error for {symbol}: {e}")
            return None
    
    def _check_risk_management(self, signal: TradeSignal) -> bool:
        """Check risk management rules"""
        
        # Check cooldown after loss
        if time.time() - self.last_loss_time < COOLDOWN_AFTER_LOSS:
            log.debug("In cooldown after loss")
            return False
        
        # Check max open positions
        if len(self.positions) >= MAX_OPEN_POSITIONS:
            log.debug(f"Max positions reached: {len(self.positions)}")
            return False
        
        # Check daily loss limit
        if self.stats.daily_loss_pct >= MAX_DAILY_LOSS_PCT:
            log.warning(f"Daily loss limit reached: {self.stats.daily_loss_pct:.1f}%")
            return False
        
        # Check if already have position in this symbol
        if signal.symbol in self.positions:
            log.debug(f"Already have position in {signal.symbol}")
            return False
        
        return True
    
    # ========== TRADE EXECUTION ==========
    
    async def execute_trade(self, signal: TradeSignal, portfolio_value: float = 10000.0):
        """Execute trade based on signal"""
        
        try:
            # Calculate position size
            position_size = self._calculate_position_size(signal, portfolio_value)
            
            if position_size <= 0:
                log.warning(f"Invalid position size for {signal.symbol}")
                return False
            
            # Create position
            position = Position(
                signal=signal,
                size=position_size,
                entry_time=time.time(),
                current_stop=signal.stop_loss,
                highest_price=signal.entry_price if signal.direction == "LONG" else float('inf'),
                lowest_price=signal.entry_price if signal.direction == "SHORT" else 0
            )
            
            # Update signal
            signal.status = "TRIGGERED"
            signal.entry_time = time.time()
            
            # Store position
            self.positions[signal.symbol] = position
            self.signals[signal.signal_id] = signal
            
            # Save to database
            await self._save_signal(signal)
            
            # Update stats
            self.stats.trades_executed += 1
            
            log.info(f"✅ Trade executed: {signal.symbol} {signal.direction} "
                    f"@ {signal.entry_price:.6f}, Size: ${position_size:.2f}")
            
            # Send notification
            await self._send_trade_notification(signal, position_size)
            
            return True
            
        except Exception as e:
            log.error(f"Trade execution error: {e}")
            return False
    
    def _calculate_position_size(self, signal: TradeSignal, portfolio_value: float) -> float:
        """Calculate position size based on risk"""
        risk_per_trade = portfolio_value * MAX_POSITION_SIZE
        price_risk = abs(signal.entry_price - signal.stop_loss)
        
        if price_risk > 0:
            position_size = risk_per_trade / price_risk * signal.entry_price
            return min(position_size, risk_per_trade)  # Cap at risk amount
        
        return 0.0
    
    async def _save_signal(self, signal: TradeSignal):
        """Save signal to database"""
        try:
            await self.db.execute("""
                INSERT INTO signals (
                    id, symbol, direction, status,
                    entry_price, stop_loss, take_profit, wave_target,
                    confidence, risk_reward, quality_score,
                    primary_tf, entry_tf
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal.signal_id,
                signal.symbol,
                signal.direction,
                signal.status,
                signal.entry_price,
                signal.stop_loss,
                signal.take_profit,
                signal.wave_target,
                signal.overall_confidence,
                signal.risk_reward,
                signal.quality_score,
                signal.primary_timeframe,
                signal.entry_timeframe
            ))
            
            await self.db.commit()
            
        except Exception as e:
            log.error(f"Error saving signal: {e}")
    
    # ========== POSITION MONITORING ==========
    
    async def monitor_positions(self):
        """Monitor and manage open positions"""
        log.info("👀 Starting position monitoring...")
        
        while True:
            try:
                if not self.positions:
                    await asyncio.sleep(1)
                    continue
                
                positions_to_remove = []
                
                for symbol, position in list(self.positions.items()):
                    try:
                        # Get current price
                        ticker = await self.exchange.fetch_ticker(symbol)
                        current_price = ticker['last']
                        
                        # Update price extremes
                        if position.signal.direction == "LONG":
                            position.highest_price = max(position.highest_price, current_price)
                            position.lowest_price = min(position.lowest_price, current_price)
                        else:
                            position.highest_price = min(position.highest_price, current_price)
                            position.lowest_price = max(position.lowest_price, current_price)
                        
                        # Check exit conditions
                        should_exit, exit_reason = self._check_exit_conditions(position, current_price)
                        
                        if should_exit:
                            await self._close_position(position, current_price, exit_reason)
                            positions_to_remove.append(symbol)
                        
                        # Update trailing stop
                        self._update_trailing_stop(position, current_price)
                        
                    except Exception as e:
                        log.error(f"Position monitoring error for {symbol}: {e}")
                        continue
                
                # Remove closed positions
                for symbol in positions_to_remove:
                    if symbol in self.positions:
                        del self.positions[symbol]
                
                await asyncio.sleep(2)
                
            except Exception as e:
                log.error(f"Monitoring loop error: {e}")
                await asyncio.sleep(5)
    
    def _check_exit_conditions(self, position: Position, current_price: float) -> Tuple[bool, str]:
        """Check if position should be exited"""
        signal = position.signal
        
        if signal.direction == "LONG":
            # Stop loss hit
            if current_price <= position.current_stop:
                return True, "STOP_LOSS"
            
            # Take profit hit
            if current_price >= signal.take_profit:
                return True, "TAKE_PROFIT"
            
            # Wave target hit
            if current_price >= signal.wave_target:
                return True, "WAVE_TARGET"
            
            # Time-based exit (4 hours max)
            if time.time() - position.entry_time > 4 * 3600:
                return True, "TIME_EXIT"
            
        else:  # SHORT
            if current_price >= position.current_stop:
                return True, "STOP_LOSS"
            if current_price <= signal.take_profit:
                return True, "TAKE_PROFIT"
            if current_price <= signal.wave_target:
                return True, "WAVE_TARGET"
            if time.time() - position.entry_time > 4 * 3600:
                return True, "TIME_EXIT"
        
        return False, ""
    
    def _update_trailing_stop(self, position: Position, current_price: float):
        """Update trailing stop loss"""
        signal = position.signal
        
        if signal.direction == "LONG":
            # Calculate profit percentage
            profit_pct = (current_price - signal.entry_price) / signal.entry_price
            
            # Activate trailing stop
            if profit_pct >= TRAILING_STOP_ACTIVATE and not position.trailing_active:
                position.trailing_active = True
                log.info(f"🔄 Trailing stop activated for {signal.symbol}")
            
            # Update trailing stop
            if position.trailing_active:
                new_stop = current_price * (1 - TRAILING_STOP_DISTANCE)
                if new_stop > position.current_stop:
                    position.current_stop = new_stop
        
        else:  # SHORT
            profit_pct = (signal.entry_price - current_price) / signal.entry_price
            
            if profit_pct >= TRAILING_STOP_ACTIVATE and not position.trailing_active:
                position.trailing_active = True
                log.info(f"🔄 Trailing stop activated for {signal.symbol}")
            
            if position.trailing_active:
                new_stop = current_price * (1 + TRAILING_STOP_DISTANCE)
                if new_stop < position.current_stop:
                    position.current_stop = new_stop
    
    async def _close_position(self, position: Position, exit_price: float, reason: str):
        """Close position and update statistics"""
        try:
            signal = position.signal
            
            # Calculate P&L
            if signal.direction == "LONG":
                pnl_pct = (exit_price - signal.entry_price) / signal.entry_price * 100
            else:
                pnl_pct = (signal.entry_price - exit_price) / signal.entry_price * 100
            
            # Update signal
            signal.status = "CLOSED"
            signal.exit_time = time.time()
            signal.exit_price = exit_price
            signal.pnl_pct = pnl_pct
            signal.exit_reason = reason
            
            # Update database
            await self.db.execute("""
                UPDATE signals SET
                    status = ?,
                    closed_at = CURRENT_TIMESTAMP,
                    exit_price = ?,
                    pnl_pct = ?,
                    exit_reason = ?
                WHERE id = ?
            """, ("CLOSED", exit_price, pnl_pct, reason, signal.signal_id))
            
            await self.db.commit()
            
            # Update statistics
            self._update_performance_stats(pnl_pct)
            
            # Send notification
            await self._send_exit_notification(signal, pnl_pct, reason)
            
            log.info(f"📤 Position closed: {signal.symbol} {reason} "
                    f"({pnl_pct:+.2f}%)")
            
            # Update last loss time if it was a loss
            if pnl_pct < 0:
                self.last_loss_time = time.time()
            
        except Exception as e:
            log.error(f"Error closing position: {e}")
    
    def _update_performance_stats(self, pnl_pct: float):
        """Update performance statistics"""
        # Update daily P&L
        self.stats.daily_pnl_pct += pnl_pct
        
        if pnl_pct < 0:
            self.stats.daily_loss_pct += abs(pnl_pct)
            self.stats.losing_trades += 1
            if self.stats.is_winning_streak:
                self.stats.current_streak = 1
                self.stats.is_winning_streak = False
            else:
                self.stats.current_streak += 1
                self.stats.max_consecutive_losses = max(
                    self.stats.max_consecutive_losses, self.stats.current_streak
                )
        else:
            self.stats.winning_trades += 1
            if not self.stats.is_winning_streak:
                self.stats.current_streak = 1
                self.stats.is_winning_streak = True
            else:
                self.stats.current_streak += 1
                self.stats.max_consecutive_wins = max(
                    self.stats.max_consecutive_wins, self.stats.current_streak
                )
        
        self.stats.total_pnl_pct += pnl_pct
    
    # ========== NOTIFICATIONS ==========
    
    async def _send_trade_notification(self, signal: TradeSignal, position_size: float):
        """Send trade entry notification"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return
        
        try:
            side_emoji = "🟢" if signal.direction == "LONG" else "🔴"
            side_text = "شراء" if signal.direction == "LONG" else "بيع"
            
            # Calculate wave potential
            wave_potential = abs(signal.wave_target - signal.entry_price) / signal.entry_price * 100
            
            message = f"""
{side_emoji} <b>إدخال صفقة - نظام التاجر الأساسي</b>

<b>{signal.symbol}</b> | {side_text}

<b>📊 التحليل:</b>
1️⃣ <b>الاتجاه:</b> {signal.market_direction.primary_bias} ({signal.market_direction.confidence:.1%})
2️⃣ <b>الموجة:</b> {signal.wave_energy.wave_type}/{signal.wave_energy.wave_stage}
3️⃣ <b>القوة:</b> {signal.strength_volume.strength_score:.1%}
4️⃣ <b>المؤشرات:</b> RSI={signal.indicators.rsi_position:.1f}, EMA={signal.indicators.ema_order}

<b>⚡ التنفيذ:</b>
‎• سعر الدخول: <code>{signal.entry_price:.6f}</code>
‎• وقف الخسارة: <code>{signal.stop_loss:.6f}</code>
‎• هدف الربح: <code>{signal.take_profit:.6f}</code>
‎• الهدف الموجي: <code>{signal.wave_target:.6f}</code> ({wave_potential:+.1f}%)
‎• حجم الصفقة: ${position_size:.2f}

<b>🎯 الجودة:</b>
‎• الثقة الكلية: {signal.overall_confidence:.1%}
‎• جودة الإشارة: {signal.quality_score:.1%}
‎• نسبة المخاطرة: {signal.risk_reward:.2f}:1

<b>🧠 عقلية التاجر:</b>
تم تحديد الاتجاه أولاً
تم تأكيد القوة والفوليوم
تم ضبط التوقيت بالمؤشرات
تنفيذ عند تأكيد الدفع

#صفقة_جديدة #نظام_التاجر #{'شراء' if signal.direction == 'LONG' else 'بيع'}
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True
                })
                
        except Exception as e:
            log.error(f"Trade notification error: {e}")
    
    async def _send_exit_notification(self, signal: TradeSignal, pnl_pct: float, reason: str):
        """Send trade exit notification"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return
        
        try:
            side_emoji = "🟢" if signal.direction == "LONG" else "🔴"
            side_text = "شراء" if signal.direction == "LONG" else "بيع"
            
            # Determine result emoji
            if pnl_pct > 0:
                result_emoji = "💰"
                result_text = f"ربح +{pnl_pct:.2f}%"
            else:
                result_emoji = "⚠️"
                result_text = f"خسارة {pnl_pct:.2f}%"
            
            # Reason mapping
            reason_map = {
                "STOP_LOSS": "وقف الخسارة",
                "TAKE_PROFIT": "هدف الربح",
                "WAVE_TARGET": "الهدف الموجي",
                "TIME_EXIT": "انتهاء الوقت"
            }
            
            reason_text = reason_map.get(reason, reason)
            
            message = f"""
{result_emoji} <b>إغلاق صفقة</b>

<b>{signal.symbol}</b> | {side_text}

<b>📊 النتيجة:</b> {result_text}
<b>🎯 السبب:</b> {reason_text}

<b>📈 الإحصائيات:</b>
‎• سعر الدخول: <code>{signal.entry_price:.6f}</code>
‎• سعر الخروج: <code>{signal.exit_price:.6f}</code>
‎• المدة: {int((signal.exit_time - signal.entry_time) / 60)} دقيقة

<b>🧠 العبرة:</b>
كل صفقة لها بداية ونهاية
الأهم هو استمرارية النظام
الصبر على النتائج الإجمالية

#إغلاق_صفقة #{'ربح' if pnl_pct > 0 else 'خسارة'} #{reason_text.replace(' ', '_')}
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True
                })
                
        except Exception as e:
            log.error(f"Exit notification error: {e}")
    
    # ========== UTILITY METHODS ==========
    
    def _default_wave_energy(self) -> WaveEnergy:
        return WaveEnergy(
            wave_type="UNKNOWN",
            wave_stage="UNKNOWN",
            wave_length_score=0.5,
            energy_level=0.5,
            compression_ratio=0.5,
            momentum_gradient=0.0,
            volume_profile=0.5,
            candle_consistency=0.5
        )
    
    def _default_strength_volume(self) -> StrengthVolume:
        return StrengthVolume(
            strength_score=0.5,
            volume_score=0.5,
            market_participation=0.5,
            body_dominance=0.5,
            volume_expansion=1.0,
            bid_ask_balance=0.0
        )
    
    def _default_indicators(self) -> IndicatorAlignment:
        return IndicatorAlignment(
            rsi_position=50.0,
            rsi_momentum=0.0,
            ema_alignment=0.5,
            volume_trend=0.0,
            ema_order="MIXED",
            price_vs_ema="BETWEEN",
            macd_signal="NEUTRAL"
        )
    
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

# ================ MAIN SCANNER ================
class TraderCoreScanner:
    """Main scanner system"""
    
    def __init__(self):
        self.engine = TraderCoreEngine()
        self.is_running = False
        self.portfolio_value = 10000.0  # Default portfolio value
    
    async def initialize(self):
        """Initialize scanner"""
        log.info("=" * 70)
        log.info("🎯 TRADER'S CORE LOGIC SYSTEM - INITIALIZING")
        log.info("=" * 70)
        log.info("STEP 1: Watch all timeframes → Determine direction")
        log.info("STEP 2: Wave length → Estimate potential")
        log.info("STEP 3: Strength + Volume → Confirm move")
        log.info("STEP 4: Indicators → Fine-tune timing")
        log.info("STEP 5: Decide direction → Execute")
        log.info("=" * 70)
        log.info(f"Exchange: {EXCHANGE_NAME}")
        log.info(f"Max Positions: {MAX_OPEN_POSITIONS}")
        log.info(f"Risk per Trade: {MAX_POSITION_SIZE*100:.1f}%")
        log.info(f"Min Confidence: {MIN_CONFIDENCE:.0%}")
        log.info(f"Min Risk:Reward: {MIN_RISK_REWARD:.1f}:1")
        log.info("=" * 70)
        
        await self.engine.initialize()
        
        # Send startup notification
        await self._send_startup_notification()
    
    async def _send_startup_notification(self):
        """Send startup notification"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return
        
        try:
            message = f"""
🚀 <b>نظام التاجر الأساسي - التشغيل</b>

<b>🎯 الفلسفة:</b>
1. شاهد جميع الفريمات ← حدد الاتجاه
2. حلل طول الموجة ← قدر الإمكانية
3. تأكد من القوة والفوليوم ← تحقق من حقيقة الحركة
4. راجع المؤشرات ← اضبط التوقيت
5. اتخذ القرار ← نفذ

<b>⚙️ الإعدادات:</b>
• البورصة: {EXCHANGE_NAME}
• الحد الأقصى للصفقات المفتوحة: {MAX_OPEN_POSITIONS}
• المخاطرة لكل صفقة: {MAX_POSITION_SIZE*100:.1f}%
• الحد الأدنى للثقة: {MIN_CONFIDENCE:.0%}
• الحد الأدنى للمخاطرة: {MIN_RISK_REWARD:.1f}:1

<b>🧠 عقلية التاجر:</b>
أنا صياد طاقة، لا أنتظر انتقالات
أحدد الاتجاه أولاً، ثم أدخل عند تأكيد الدفع
الجودة فوق الكمية، النظام فوق العاطفة

<b>✅ النظام جاهز للعمل...</b>

#تشغيل_النظام #تاجر_أساسي #عقلية_محترف
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True
                })
                
        except Exception as e:
            log.error(f"Startup notification error: {e}")
    
    async def scan_cycle(self):
        """Execute a single scan cycle"""
        try:
            # Get symbols to scan
            symbols = await self.engine.get_trading_symbols()
            
            if not symbols:
                log.warning("No symbols to scan")
                return
            
            log.info(f"🔍 Scanning {len(symbols)} symbols")
            
            # Scan symbols in batches
            batch_size = 5
            for i in range(0, len(symbols), batch_size):
                batch = symbols[i:i + batch_size]
                
                tasks = []
                for symbol in batch:
                    task = self._scan_symbol(symbol)
                    tasks.append(task)
                
                await asyncio.gather(*tasks, return_exceptions=True)
                await asyncio.sleep(0.5)  # Rate limiting
            
            # Log statistics
            self._log_scan_stats()
            
        except Exception as e:
            log.error(f"Scan cycle error: {e}")
    
    async def _scan_symbol(self, symbol: str):
        """Scan a single symbol"""
        try:
            # Skip if already in position
            if symbol in self.engine.positions:
                return
            
            # Fetch data
            multi_tf_data = await self.engine.fetch_multi_tf_data(symbol)
            
            if not multi_tf_data or len(multi_tf_data) < 3:
                return
            
            # Generate signal
            signal = await self.engine.generate_trade_signal(symbol, multi_tf_data)
            
            if signal:
                # Execute trade
                await self.engine.execute_trade(signal, self.portfolio_value)
                
        except Exception as e:
            log.debug(f"Symbol scan error {symbol}: {str(e)[:50]}")
    
    def _log_scan_stats(self):
        """Log scanning statistics"""
        stats = self.engine.stats
        positions = len(self.engine.positions)
        
        log.info(f"📊 Scan Stats: Total={stats.total_scans}, "
                f"Signals={stats.signals_found}, "
                f"Trades={stats.trades_executed}, "
                f"Positions={positions}")
        
        if stats.trades_executed > 0:
            win_rate = stats.winning_trades / stats.trades_executed * 100
            log.info(f"   Performance: Win Rate={win_rate:.1f}%, "
                    f"Total P&L={stats.total_pnl_pct:.2f}%, "
                    f"Daily={stats.daily_pnl_pct:.2f}%")
    
    async def run(self):
        """Main run loop"""
        try:
            await self.initialize()
            self.is_running = True
            
            log.info("🚀 Starting Trader's Core Logic System")
            
            # Start monitoring in background
            monitoring_task = asyncio.create_task(self.engine.monitor_positions())
            
            # Main scanning loop
            scan_count = 0
            while self.is_running:
                try:
                    scan_count += 1
                    log.info(f"🔄 Scan cycle #{scan_count}")
                    
                    await self.scan_cycle()
                    
                    # Wait for next scan
                    log.info(f"⏳ Next scan in {SCAN_INTERVAL} seconds...")
                    await asyncio.sleep(SCAN_INTERVAL)
                    
                except KeyboardInterrupt:
                    log.info("⏸️ Scanner paused by user")
                    self.is_running = False
                    break
                except Exception as e:
                    log.error(f"Scan loop error: {e}")
                    await asyncio.sleep(5)
            
            # Wait for monitoring to finish
            monitoring_task.cancel()
            try:
                await monitoring_task
            except asyncio.CancelledError:
                pass
            
            await self.shutdown()
            
        except Exception as e:
            log.error(f"Scanner error: {e}")
            await self.shutdown()
    
    async def shutdown(self):
        """Shutdown scanner"""
        log.info("🛑 Shutting down Trader's Core Logic System")
        
        # Close all open positions
        await self._close_all_positions()
        
        # Send final statistics
        await self._send_final_stats()
        
        # Cleanup
        await self.engine.cleanup()
        
        log.info("✅ Scanner shut down successfully")
    
    async def _close_all_positions(self):
        """Close all open positions"""
        if not self.engine.positions:
            return
        
        log.info(f"🔒 Closing {len(self.engine.positions)} open positions")
        
        for symbol, position in list(self.engine.positions.items()):
            try:
                # Get current price
                ticker = await self.engine.exchange.fetch_ticker(symbol)
                current_price = ticker['last']
                
                # Close position
                await self.engine._close_position(position, current_price, "SHUTDOWN")
                
            except Exception as e:
                log.error(f"Error closing position {symbol}: {e}")
    
    async def _send_final_stats(self):
        """Send final statistics"""
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return
        
        try:
            stats = self.engine.stats
            
            if stats.trades_executed > 0:
                win_rate = stats.winning_trades / stats.trades_executed * 100
                avg_pnl = stats.total_pnl_pct / stats.trades_executed if stats.trades_executed > 0 else 0
            else:
                win_rate = 0
                avg_pnl = 0
            
            message = f"""
📊 <b>إحصائيات نهائية - نظام التاجر الأساسي</b>

<b>🎯 الأداء:</b>
‎• إجمالي الصفقات: {stats.trades_executed}
‎• الصفقات الرابحة: {stats.winning_trades}
‎• الصفقات الخاسرة: {stats.losing_trades}
‎• نسبة الربح: {win_rate:.1f}%
‎• متوسط الربح/الصفقة: {avg_pnl:+.2f}%
‎• إجمالي الربح: {stats.total_pnl_pct:+.2f}%

<b>📈 المسح:</b>
‎• دورات المسح: {stats.total_scans}
‎• الإشارات المكتشفة: {stats.signals_found}
‎• الصفقات المنفذة: {stats.trades_executed}

<b>🔥 السلاسل:</b>
‎• أطول سلسلة رابحة: {stats.max_consecutive_wins}
‎• أطول سلسلة خاسرة: {stats.max_consecutive_losses}

<b>🧠 التقييم:</b>
النظام نفذ فلسفة التاجر الأساسي
حدد الاتجاه أولاً، ثم نفذ عند التأكيد
الحفاظ على الجودة فوق الكمية

<b>✅ إغلاق النظام...</b>

#إحصائيات #تاجر_أساسي #نهاية_الجلسة
"""
            
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True
                })
                
        except Exception as e:
            log.error(f"Final stats error: {e}")

# ================ MAIN ENTRY POINT ================
async def main():
    """Main function"""
    scanner = TraderCoreScanner()
    
    try:
        await scanner.run()
    except KeyboardInterrupt:
        log.info("\n👋 Scanner stopped by user")
    except Exception as e:
        log.error(f"Main error: {e}")
        traceback.print_exc()
    finally:
        log.info("🎯 Trader's Core Logic System terminated")

if __name__ == "__main__":
    # Set event loop policy for Windows compatibility
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    # Run the scanner
    asyncio.run(main())