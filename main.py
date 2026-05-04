#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ROMEOTPT SCANNER v6.1 – MEXC FUTURES (PERPETUAL SWAP) EDITION
Multi-Timeframe Explosive Move Detection System
PRIMARY METHOD: SMA Trend → Wave ABC Correction → RSI Divergence → MACD → Volume Breakout
+ Fast Scalp Engine + Order‑Book Filter + Live Outcome Alerts
Exchange: MEXC USDT Perpetual Futures
Top 300 coins scanned every cycle
"""

import os
import time
import asyncio
import logging
import datetime
import math
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
import numpy as np
from fastapi import FastAPI
import uvicorn
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from enum import Enum

# ============ ENUMS ============
class TrendBias(str, Enum):
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"
    NEUTRAL = "NEUTRAL"

class WavePattern(str, Enum):
    ABC_CORRECTION = "ABC_CORRECTION"
    FALLING_WEDGE = "FALLING_WEDGE"
    RISING_WEDGE = "RISING_WEDGE"
    BULL_FLAG = "BULL_FLAG"
    BEAR_FLAG = "BEAR_FLAG"
    NONE = "NONE"

class DivergenceType(str, Enum):
    BULLISH_REGULAR = "BULLISH_REGULAR"
    BEARISH_REGULAR = "BEARISH_REGULAR"
    HIDDEN_BULLISH = "HIDDEN_BULLISH"
    HIDDEN_BEARISH = "HIDDEN_BEARISH"
    NONE = "NONE"

class DirectionTier(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"

class TrappedSide(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    NONE = "NONE"
    CONFLICT = "CONFLICT"

class MicroConfirmationType(str, Enum):
    WICK_REJECTION = "WICK_REJECTION"
    ABSORPTION = "ABSORPTION"
    BREAKOUT = "BREAKOUT"
    NONE = "NONE"

# ---------------- CONFIG ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
DB_PATH = os.getenv("DB_PATH", "/app/data/romeopt_v6_1_mexc.db")

# Scanner settings
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 45))
TOP_N = int(os.getenv("TOP_N", 100))
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", 2))

# Wave Momentum Engine thresholds
MIN_FIB_RETRACEMENT = float(os.getenv("MIN_FIB_RETRACEMENT", 0.5))
OPTIMAL_FIB_ZONE_MIN = float(os.getenv("OPTIMAL_FIB_ZONE_MIN", 0.618))
OPTIMAL_FIB_ZONE_MAX = float(os.getenv("OPTIMAL_FIB_ZONE_MAX", 0.705))
MIN_DIVERGENCE_STRENGTH = float(os.getenv("MIN_DIVERGENCE_STRENGTH", 0.6))
VOLUME_SPIKE_MULTIPLIER = float(os.getenv("VOLUME_SPIKE_MULTIPLIER", 2.0))

# Direction Engine thresholds
MIN_DIRECTION_CONFIDENCE = float(os.getenv("MIN_DIRECTION_CONFIDENCE", 0.4))
FUNDING_EXTREME_THRESHOLD = float(os.getenv("FUNDING_EXTREME_THRESHOLD", 0.03))
OI_ACCUMULATION_THRESHOLD = float(os.getenv("OI_ACCUMULATION_THRESHOLD", 0.15))

# Signal thresholds
MIN_QUALITY_SCORE = float(os.getenv("MIN_QUALITY_SCORE", 0.0))

# Deduplication settings
SIGNAL_COOLDOWN_MINUTES = int(os.getenv("SIGNAL_COOLDOWN_MINUTES", 15))
SIGNAL_VALIDITY_HOURS = int(os.getenv("SIGNAL_VALIDITY_HOURS", 48))

# Rate limiting – MEXC allows 20 req/s, we stay around 10 req/s max
MAX_REQUESTS_PER_SECOND = int(os.getenv("MAX_REQUESTS_PER_SECOND", 10))
RATE_LIMIT_RETRIES = int(os.getenv("RATE_LIMIT_RETRIES", 3))
RATE_LIMIT_BACKOFF_FACTOR = float(os.getenv("RATE_LIMIT_BACKOFF_FACTOR", 2.5))

# Outcome monitor settings
OUTCOME_CHECK_INTERVAL = int(os.getenv("OUTCOME_CHECK_INTERVAL", 30))

# ---------------- LOGGING ----------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("romeopt_mexc_v6.1")

# ============ DATA STRUCTURES ============
@dataclass
class WaveStructure:
    pattern: WavePattern = WavePattern.NONE
    pattern_confidence: float = 0.0
    impulse_start: float = 0.0
    impulse_end: float = 0.0
    impulse_size_pct: float = 0.0
    correction_start: float = 0.0
    correction_end: float = 0.0
    correction_size_pct: float = 0.0
    fib_236: float = 0.0
    fib_382: float = 0.0
    fib_500: float = 0.0
    fib_618: float = 0.0
    fib_705: float = 0.0
    fib_786: float = 0.0
    current_retracement: float = 0.0
    in_optimal_zone: bool = False
    distance_to_zone_pct: float = 999.0
    zone_price_high: float = 0.0
    zone_price_low: float = 0.0
    swing_points: List[Dict] = field(default_factory=list)
    candle_count: int = 0

@dataclass
class MomentumSignals:
    divergence_type: DivergenceType = DivergenceType.NONE
    divergence_strength: float = 0.0
    divergence_points: List[Dict] = field(default_factory=list)
    rsi_current: float = 50.0
    rsi_at_price_low: float = 50.0
    rsi_at_price_high: float = 50.0
    macd_crossed: bool = False
    macd_cross_direction: str = ""
    macd_histogram_reversal: bool = False
    macd_line: float = 0.0
    macd_signal_line: float = 0.0
    macd_histogram: float = 0.0
    momentum_score: float = 0.0
    momentum_aligned: bool = False

@dataclass
class VolumeBreakout:
    triggered: bool = False
    breakout_candle_volume: float = 0.0
    avg_volume_20: float = 0.0
    volume_ratio: float = 0.0
    breakout_direction: str = ""
    breakout_price: float = 0.0
    pattern_break: bool = False
    fakeout_detected: bool = False
    sweep_then_reclaim: bool = False
    volume_score: float = 0.0

@dataclass
class InstitutionalData:
    open_interest: float = 0.0
    oi_change_24h: float = 0.0
    oi_change_1h: float = 0.0
    oi_timestamp: Optional[datetime.datetime] = None
    funding_rate: float = 0.0
    funding_history: List[float] = field(default_factory=list)
    funding_timestamp: Optional[datetime.datetime] = None
    basis_rate: float = 0.0
    perpetual_premium: float = 0.0
    top_bid_size: float = 0.0
    top_ask_size: float = 0.0
    bid_ask_ratio: float = 0.0
    liquidation_zones: Dict[str, List[float]] = field(default_factory=dict)
    
    @property
    def is_funding_extreme(self) -> bool:
        return abs(self.funding_rate) > FUNDING_EXTREME_THRESHOLD
    
    @property
    def funding_bleeding_side(self) -> str:
        if self.funding_rate > FUNDING_EXTREME_THRESHOLD:
            return "LONG"
        elif self.funding_rate < -FUNDING_EXTREME_THRESHOLD:
            return "SHORT"
        return ""

@dataclass
class DirectionMetrics:
    trapped_side: TrappedSide = TrappedSide.NONE
    trapped_confidence: float = 0.0
    trapped_details: List[Dict] = field(default_factory=list)
    bleeding_side: str = ""
    funding_extreme: float = 0.0
    funding_analysis: Dict = field(default_factory=dict)
    micro_confirmation: bool = False
    micro_timeframe: str = ""
    rejection_type: MicroConfirmationType = MicroConfirmationType.NONE
    micro_details: Dict = field(default_factory=dict)
    orderbook_imbalance: float = 0.0
    direction_score: float = 0.0
    confidence_tier: DirectionTier = DirectionTier.LOW
    conflict_warnings: List[str] = field(default_factory=list)
    
    @property
    def is_high_confidence(self) -> bool:
        return (self.confidence_tier == DirectionTier.HIGH and 
                abs(self.direction_score) > 0.7)
    
    @property
    def has_major_conflicts(self) -> bool:
        return len(self.conflict_warnings) >= 2

# ---------------- ENHANCED RATE LIMITER ----------------
class EnhancedRateLimiter:
    def __init__(self):
        self.max_rps = MAX_REQUESTS_PER_SECOND
        self.max_concurrent = MAX_CONCURRENT
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        self.general_requests = []
        self.funding_requests = []
        self.oi_requests = []
        self.min_delay = 0.1
        self.backoff_factor = RATE_LIMIT_BACKOFF_FACTOR
        self.max_retries = RATE_LIMIT_RETRIES
        
    async def wait_for_endpoint(self, endpoint_type: str = "general"):
        now = time.time()
        if endpoint_type == "funding":
            request_list = self.funding_requests
            cooldown = 1.2
        elif endpoint_type == "oi":
            request_list = self.oi_requests
            cooldown = 1.5
        else:
            request_list = self.general_requests
            cooldown = 0.8
        
        request_list[:] = [t for t in request_list if now - t < cooldown]
        
        if len(request_list) >= 1:
            wait_time = cooldown - (now - request_list[0])
            if wait_time > 0:
                wait_time += np.random.uniform(0.05, 0.15)
                await asyncio.sleep(wait_time)
        
        request_list.append(now)
        await asyncio.sleep(0.05)
    
    async def execute_with_backoff(self, func, *args, endpoint_type="general", **kwargs):
        async with self.semaphore:
            for attempt in range(self.max_retries):
                try:
                    await self.wait_for_endpoint(endpoint_type)
                    result = await func(*args, **kwargs)
                    extra_delay = {
                        "funding": 0.1,
                        "oi": 0.15,
                        "general": 0.03
                    }.get(endpoint_type, 0.03)
                    await asyncio.sleep(extra_delay)
                    return result
                except Exception as e:
                    error_str = str(e)
                    if any(phrase in error_str for phrase in ["Too Many Requests", "50011", "429", "rate limit", "Rate limit"]):
                        wait_time = self.min_delay * (self.backoff_factor ** attempt)
                        wait_time += np.random.uniform(0.2, 0.5)
                        log.warning(f"Rate limited on {endpoint_type}, attempt {attempt+1}/{self.max_retries}, waiting {wait_time:.2f}s")
                        await asyncio.sleep(wait_time)
                    else:
                        raise e
            raise Exception(f"Failed after {self.max_retries} retries")

rate_limiter = EnhancedRateLimiter()

# ---------------- UTILS ----------------
async def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit: int = 100):
    log.debug(f"Fetching OHLCV {symbol} {timeframe}")
    try:
        result = await rate_limiter.execute_with_backoff(
            exchange.fetch_ohlcv, symbol, timeframe=timeframe, limit=limit
        )
        return result
    except Exception as e:
        log.warning(f"Failed to fetch {symbol} {timeframe}: {e}")
        return None

def create_dataframe(ohlcv):
    if not ohlcv:
        return None
    df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

async def safe_fetch_ticker(exchange, symbol: str):
    try:
        return await rate_limiter.execute_with_backoff(exchange.fetch_ticker, symbol)
    except Exception as e:
        log.debug(f"Failed to fetch ticker for {symbol}: {e}")
        return None

async def safe_fetch_tickers(exchange):
    try:
        return await rate_limiter.execute_with_backoff(exchange.fetch_tickers)
    except Exception as e:
        log.error(f"Failed to fetch tickers: {e}")
        return {}

# ============ WAVE RANGE DETECTOR ============
class WaveRangeDetector:
    def __init__(self):
        self.min_wave_size_pct = 2.0
        self.max_correction_pct = 0.80
    
    def detect_trend_bias(self, df_daily, df_4h) -> Tuple[TrendBias, float]:
        if df_daily is None or df_4h is None:
            return TrendBias.NEUTRAL, 0.0
        try:
            df_dc = df_daily.copy()
            df_dc['sma_50'] = df_dc['close'].rolling(50).mean()
            df_dc['sma_200'] = df_dc['close'].rolling(200).mean()
            current_price = df_dc['close'].iloc[-1]
            sma50d = df_dc['sma_50'].iloc[-1]
            sma200d = df_dc['sma_200'].iloc[-1]
            df_4c = df_4h.copy()
            df_4c['sma_50'] = df_4c['close'].rolling(50).mean()
            df_4c['sma_200'] = df_4c['close'].rolling(200).mean()
            sma50_4h = df_4c['sma_50'].iloc[-1]
            sma200_4h = df_4c['sma_200'].iloc[-1]
            price_4h = df_4c['close'].iloc[-1]
            daily_bull = current_price > sma50d and current_price > sma200d
            h4_bull = price_4h > sma50_4h and price_4h > sma200_4h
            daily_bear = current_price < sma50d and current_price < sma200d
            h4_bear = price_4h < sma50_4h and price_4h < sma200_4h
            if daily_bull and h4_bull: return TrendBias.BULLISH, 1.0
            if daily_bear and h4_bear: return TrendBias.BEARISH, 1.0
            if daily_bull and price_4h > sma50_4h: return TrendBias.BULLISH, 0.7
            if daily_bear and price_4h < sma50_4h: return TrendBias.BEARISH, 0.7
            if current_price > sma50d and current_price > sma200d: return TrendBias.BULLISH, 0.5
            if current_price < sma50d and current_price < sma200d: return TrendBias.BEARISH, 0.5
            return TrendBias.NEUTRAL, 0.0
        except Exception as e:
            log.debug(f"Trend bias error: {e}")
            return TrendBias.NEUTRAL, 0.0
    
    def identify_abc_correction(self, df_4h, trend_bias: TrendBias) -> WaveStructure:
        wave = WaveStructure()
        if df_4h is None or len(df_4h) < 30:
            return wave
        try:
            highs = df_4h['high'].values
            lows = df_4h['low'].values
            closes = df_4h['close'].values
            if trend_bias == TrendBias.BULLISH:
                return self._identify_bullish_correction(df_4h, highs, lows, closes)
            elif trend_bias == TrendBias.BEARISH:
                return self._identify_bearish_correction(df_4h, highs, lows, closes)
        except Exception as e:
            log.debug(f"ABC detection error: {e}")
        return wave
    
    def _identify_bullish_correction(self, df_4h, highs, lows, closes):
        wave = WaveStructure()
        try:
            sw_highs = self._find_swings(highs, True)
            sw_lows = self._find_swings(lows, False)
            if len(sw_highs) < 1 or len(sw_lows) < 2: return wave
            shs = sorted(sw_highs, key=lambda x: x['index'])
            sls = sorted(sw_lows, key=lambda x: x['index'])
            for sh in reversed(shs[-5:]):
                impulse_end = sh['price']; idx_end = sh['index']
                prev_l = [sl for sl in sls if sl['index'] < idx_end]
                if not prev_l: continue
                impulse_start = prev_l[-1]['price']; idx_start = prev_l[-1]['index']
                imp_pct = abs(impulse_end - impulse_start)/impulse_start*100
                if imp_pct < self.min_wave_size_pct: continue
                later_lows = [sl for sl in sls if sl['index'] > idx_end]
                if not later_lows: continue
                correction_end = later_lows[0]['price']
                fib_range = impulse_end - impulse_start
                if fib_range <= 0: continue
                retrace = (impulse_end - correction_end) / fib_range
                if not (0.5 <= retrace <= 0.8): continue
                wave.pattern = WavePattern.ABC_CORRECTION
                wave.pattern_confidence = min(1.0, imp_pct/5.0) * min(1.0, retrace)
                wave.impulse_start = impulse_start
                wave.impulse_end = impulse_end
                wave.impulse_size_pct = imp_pct
                wave.correction_start = impulse_end
                wave.correction_end = correction_end
                wave.fib_236 = impulse_end - fib_range*0.236
                wave.fib_382 = impulse_end - fib_range*0.382
                wave.fib_500 = impulse_end - fib_range*0.5
                wave.fib_618 = impulse_end - fib_range*0.618
                wave.fib_705 = impulse_end - fib_range*0.705
                wave.fib_786 = impulse_end - fib_range*0.786
                wave.current_retracement = retrace
                curr_close = closes[-1]
                wave.zone_price_high = wave.fib_500
                wave.zone_price_low = wave.fib_705
                if wave.fib_705 <= curr_close <= wave.fib_500:
                    wave.in_optimal_zone = True
                if curr_close > wave.fib_500:
                    wave.distance_to_zone_pct = (curr_close - wave.fib_500)/wave.fib_500*100
                elif curr_close < wave.fib_705:
                    wave.distance_to_zone_pct = (wave.fib_705 - curr_close)/wave.fib_705*100
                else:
                    wave.distance_to_zone_pct = 0.0
                if retrace > 0.618 and curr_close <= wave.fib_618: wave.pattern = WavePattern.FALLING_WEDGE
                elif self._is_flag(df_4h, True): wave.pattern = WavePattern.BULL_FLAG
                break
        except Exception as e:
            log.debug(f"bullish correction error: {e}")
        return wave
    
    def _identify_bearish_correction(self, df_4h, highs, lows, closes):
        wave = WaveStructure()
        try:
            sw_highs = self._find_swings(highs, True)
            sw_lows = self._find_swings(lows, False)
            if len(sw_highs) < 2 or len(sw_lows) < 1: return wave
            shs = sorted(sw_highs, key=lambda x: x['index'])
            sls = sorted(sw_lows, key=lambda x: x['index'])
            for sl in reversed(sls[-5:]):
                impulse_end = sl['price']; idx_end = sl['index']
                prev_highs = [sh for sh in shs if sh['index'] < idx_end]
                if not prev_highs: continue
                impulse_start = prev_highs[-1]['price']
                imp_pct = abs(impulse_start - impulse_end)/impulse_start*100
                if imp_pct < self.min_wave_size_pct: continue
                later_highs = [sh for sh in shs if sh['index'] > idx_end]
                if not later_highs: continue
                correction_end = later_highs[0]['price']
                fib_range = impulse_start - impulse_end
                if fib_range <= 0: continue
                retrace = (correction_end - impulse_end) / fib_range
                if not (0.5 <= retrace <= 0.8): continue
                wave.pattern = WavePattern.ABC_CORRECTION
                wave.pattern_confidence = min(1.0, imp_pct/5.0)*min(1.0, retrace)
                wave.impulse_start = impulse_start
                wave.impulse_end = impulse_end
                wave.impulse_size_pct = imp_pct
                wave.correction_start = impulse_end
                wave.correction_end = correction_end
                wave.fib_236 = impulse_end + fib_range*0.236
                wave.fib_382 = impulse_end + fib_range*0.382
                wave.fib_500 = impulse_end + fib_range*0.5
                wave.fib_618 = impulse_end + fib_range*0.618
                wave.fib_705 = impulse_end + fib_range*0.705
                wave.fib_786 = impulse_end + fib_range*0.786
                wave.current_retracement = retrace
                curr_close = closes[-1]
                wave.zone_price_high = wave.fib_705
                wave.zone_price_low = wave.fib_500
                if wave.fib_500 <= curr_close <= wave.fib_705:
                    wave.in_optimal_zone = True
                if curr_close < wave.fib_500:
                    wave.distance_to_zone_pct = (wave.fib_500 - curr_close)/wave.fib_500*100
                elif curr_close > wave.fib_705:
                    wave.distance_to_zone_pct = (curr_close - wave.fib_705)/wave.fib_705*100
                else:
                    wave.distance_to_zone_pct = 0.0
                if retrace > 0.618 and curr_close >= wave.fib_618: wave.pattern = WavePattern.RISING_WEDGE
                elif self._is_flag(df_4h, False): wave.pattern = WavePattern.BEAR_FLAG
                break
        except Exception as e:
            log.debug(f"bearish correction error: {e}")
        return wave
    
    def _find_swings(self, prices, is_high, window=3):
        pts = []
        for i in range(window, len(prices)-window):
            if is_high:
                if prices[i] == max(prices[i-window:i+window+1]):
                    left = all(prices[i] > prices[j] for j in range(i-window, i))
                    right = all(prices[i] > prices[j] for j in range(i+1, i+window+1))
                    if left or right: pts.append({'index': i, 'price': prices[i]})
            else:
                if prices[i] == min(prices[i-window:i+window+1]):
                    left = all(prices[i] < prices[j] for j in range(i-window, i))
                    right = all(prices[i] < prices[j] for j in range(i+1, i+window+1))
                    if left or right: pts.append({'index': i, 'price': prices[i]})
        return pts
    
    def _is_flag(self, df, is_bull):
        if len(df) < 15: return False
        recent = df.iloc[-12:]
        h = recent['high'].values; l = recent['low'].values
        hr = max(h)-min(h); lr = max(l)-min(l)
        avg = recent['close'].mean()
        if avg > 0: return (hr+lr)/2/avg*100 < 3.0
        return False

wave_detector = WaveRangeDetector()

# ============ MOMENTUM DIVERGENCE ENGINE ============
class MomentumDivergenceEngine:
    def __init__(self):
        self.rsi_period = 14
        self.macd_fast = 12
        self.macd_slow = 26
        self.macd_signal = 9
    
    def analyze_momentum(self, df_1h, df_15m, trend_bias, wave) -> MomentumSignals:
        mom = MomentumSignals()
        if df_1h is None or df_15m is None:
            return mom
        try:
            rsi_1h = self._rsi(df_1h['close'])
            rsi_15m = self._rsi(df_15m['close'])
            macd_15m = self._macd(df_15m['close'])
            mom.rsi_current = rsi_15m[-1]
            if trend_bias == TrendBias.BULLISH:
                divt, divs, divp = self._bull_div(df_15m, rsi_15m, df_1h, rsi_1h)
            elif trend_bias == TrendBias.BEARISH:
                divt, divs, divp = self._bear_div(df_15m, rsi_15m, df_1h, rsi_1h)
            else:
                divt, divs, divp = DivergenceType.NONE, 0.0, []
            mom.divergence_type = divt
            mom.divergence_strength = divs
            mom.divergence_points = divp
            if len(macd_15m['macd_line']) >= 2:
                pm = macd_15m['macd_line'][-2]
                ps = macd_15m['signal_line'][-2]
                cm = macd_15m['macd_line'][-1]
                cs = macd_15m['signal_line'][-1]
                mom.macd_line = cm
                mom.macd_signal_line = cs
                mom.macd_histogram = macd_15m['histogram'][-1]
                if pm < ps and cm > cs:
                    mom.macd_crossed = True
                    mom.macd_cross_direction = "BULLISH"
                elif pm > ps and cm < cs:
                    mom.macd_crossed = True
                    mom.macd_cross_direction = "BEARISH"
                if len(macd_15m['histogram']) >= 3:
                    h3 = macd_15m['histogram'][-3]
                    h2 = macd_15m['histogram'][-2]
                    h1 = macd_15m['histogram'][-1]
                    if trend_bias == TrendBias.BULLISH and h3 < h2 < h1 and h3 < 0:
                        mom.macd_histogram_reversal = True
                    if trend_bias == TrendBias.BEARISH and h3 > h2 > h1 and h3 > 0:
                        mom.macd_histogram_reversal = True
            mom.momentum_score = self._mom_score(mom, trend_bias)
            if trend_bias == TrendBias.BULLISH:
                mom.momentum_aligned = (mom.divergence_type == DivergenceType.BULLISH_REGULAR and
                                        (mom.macd_crossed and mom.macd_cross_direction == "BULLISH" or mom.macd_histogram_reversal))
            elif trend_bias == TrendBias.BEARISH:
                mom.momentum_aligned = (mom.divergence_type == DivergenceType.BEARISH_REGULAR and
                                        (mom.macd_crossed and mom.macd_cross_direction == "BEARISH" or mom.macd_histogram_reversal))
        except Exception as e:
            log.debug(f"momentum error: {e}")
        return mom
    
    def _rsi(self, prices, period=14):
        delta = prices.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.rolling(period).mean()
        avg_loss = loss.rolling(period).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        rsi = 100 - (100/(1+rs))
        return rsi.fillna(50).values
    
    def _macd(self, prices):
        ema_fast = prices.ewm(span=12).mean()
        ema_slow = prices.ewm(span=26).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=9).mean()
        hist = macd_line - signal_line
        return {'macd_line': macd_line.values, 'signal_line': signal_line.values, 'histogram': hist.values}
    
    def _bull_div(self, df_15m, rsi_15m, df_1h, rsi_1h):
        try:
            prices = df_15m['low'].values
            lookback = min(50, len(prices)-5)
            rp = prices[-lookback:]
            rr = rsi_15m[-lookback:]
            lows = self._local_lows(rp)
            if len(lows) >= 2:
                last = lows[-1]
                prev = lows[-2]
                if last['price'] < prev['price'] and rr[last['index']] > rr[prev['index']]:
                    strength = min(1.0, (abs(last['price']-prev['price'])/prev['price']*100/2.0 + abs(rr[last['index']]-rr[prev['index']])/10.0))
                    return DivergenceType.BULLISH_REGULAR, strength, []
            if len(rsi_1h) >= 3 and rsi_1h[-1] > 40:
                return DivergenceType.BULLISH_REGULAR, 0.4, []
        except:
            pass
        return DivergenceType.NONE, 0.0, []
    
    def _bear_div(self, df_15m, rsi_15m, df_1h, rsi_1h):
        try:
            prices = df_15m['high'].values
            lookback = min(50, len(prices)-5)
            rp = prices[-lookback:]
            rr = rsi_15m[-lookback:]
            highs = self._local_highs(rp)
            if len(highs) >= 2:
                last = highs[-1]
                prev = highs[-2]
                if last['price'] > prev['price'] and rr[last['index']] < rr[prev['index']]:
                    strength = min(1.0, (abs(last['price']-prev['price'])/prev['price']*100/2.0 + abs(rr[prev['index']]-rr[last['index']])/10.0))
                    return DivergenceType.BEARISH_REGULAR, strength, []
            if len(rsi_1h) >= 3 and rsi_1h[-1] < 60:
                return DivergenceType.BEARISH_REGULAR, 0.4, []
        except:
            pass
        return DivergenceType.NONE, 0.0, []
    
    def _local_lows(self, prices, window=3):
        lows = []
        for i in range(window, len(prices)-window):
            if prices[i] == min(prices[i-window:i+window+1]):
                lows.append({'index': i, 'price': prices[i]})
        return lows
    
    def _local_highs(self, prices, window=3):
        highs = []
        for i in range(window, len(prices)-window):
            if prices[i] == max(prices[i-window:i+window+1]):
                highs.append({'index': i, 'price': prices[i]})
        return highs
    
    def _mom_score(self, mom, trend_bias):
        score = 0.0
        max_score = 0.0
        max_score += 0.4
        if mom.divergence_type != DivergenceType.NONE:
            score += 0.4 * mom.divergence_strength
        max_score += 0.3
        if mom.macd_crossed:
            if (trend_bias == TrendBias.BULLISH and mom.macd_cross_direction == "BULLISH") or \
               (trend_bias == TrendBias.BEARISH and mom.macd_cross_direction == "BEARISH"):
                score += 0.3
            else:
                score += 0.1
        max_score += 0.2
        if mom.macd_histogram_reversal:
            score += 0.2
        max_score += 0.1
        if trend_bias == TrendBias.BULLISH and mom.rsi_current > 40:
            score += 0.1
        elif trend_bias == TrendBias.BEARISH and mom.rsi_current < 60:
            score += 0.1
        return score / max_score if max_score > 0 else 0.0

momentum_engine = MomentumDivergenceEngine()

# ============ VOLUME BREAKOUT TRIGGER ============
class VolumeBreakoutTrigger:
    def __init__(self):
        self.min_volume_ratio = 1.5
        self.volume_lookback = 20
    
    def detect_breakout(self, df_5m, df_15m, trend_bias, wave, entry_price):
        brk = VolumeBreakout()
        if df_5m is None or len(df_5m) < self.volume_lookback + 3:
            return brk
        if df_15m is None or len(df_15m) < 5:
            return brk
        try:
            rv = df_5m['volume'].values[-(self.volume_lookback+3):]
            latest = rv[-3:]
            avg_vol = np.mean(rv[:self.volume_lookback])
            brk.avg_volume_20 = avg_vol
            if avg_vol <= 0:
                return brk
            for i in range(3):
                cvol = latest[i]
                ratio = cvol / avg_vol
                if ratio < self.min_volume_ratio:
                    continue
                try:
                    candle = df_5m.iloc[-(3-i)]
                except IndexError:
                    continue
                co = candle['open']
                cc = candle['close']
                ch = candle['high']
                cl = candle['low']
                if wave.in_optimal_zone or wave.distance_to_zone_pct < 2.0:
                    if trend_bias == TrendBias.BULLISH and (cc > wave.fib_500 or ch > wave.fib_500):
                        brk.triggered = True
                        brk.breakout_direction = "BULLISH"
                        brk.breakout_price = cc
                        brk.pattern_break = True
                        brk.volume_ratio = ratio
                        brk.breakout_candle_volume = cvol
                        brk.volume_score = min(1.0, (ratio-0.8)/3.5)
                        if cl < wave.fib_705 * 0.998:
                            brk.sweep_then_reclaim = True
                            brk.volume_score += 0.2
                            brk.volume_score = min(1.0, brk.volume_score)
                        return brk
                    if trend_bias == TrendBias.BEARISH and (cc < wave.fib_500 or cl < wave.fib_500):
                        brk.triggered = True
                        brk.breakout_direction = "BEARISH"
                        brk.breakout_price = cc
                        brk.pattern_break = True
                        brk.volume_ratio = ratio
                        brk.breakout_candle_volume = cvol
                        brk.volume_score = min(1.0, (ratio-0.8)/3.5)
                        if ch > wave.fib_705 * 1.002:
                            brk.sweep_then_reclaim = True
                            brk.volume_score += 0.2
                            brk.volume_score = min(1.0, brk.volume_score)
                        return brk
                # flag breakout
                if self._is_flag(df_15m, trend_bias == TrendBias.BULLISH):
                    fh = df_15m['high'].iloc[-5:].max()
                    fl = df_15m['low'].iloc[-5:].min()
                    if trend_bias == TrendBias.BULLISH and cc > fh:
                        brk.triggered = True
                        brk.breakout_direction = "BULLISH"
                        brk.breakout_price = cc
                        brk.pattern_break = True
                        brk.volume_ratio = ratio
                        brk.breakout_candle_volume = cvol
                        brk.volume_score = min(1.0, (ratio-0.8)/2.5)
                        brk.sweep_then_reclaim = (cl < fl * 0.998)
                        return brk
                    if trend_bias == TrendBias.BEARISH and cc < fl:
                        brk.triggered = True
                        brk.breakout_direction = "BEARISH"
                        brk.breakout_price = cc
                        brk.pattern_break = True
                        brk.volume_ratio = ratio
                        brk.breakout_candle_volume = cvol
                        brk.volume_score = min(1.0, (ratio-0.8)/2.5)
                        brk.sweep_then_reclaim = (ch > fh * 1.002)
                        return brk
            # final fallback
            for i in range(3):
                cvol = latest[i]
                ratio = cvol / avg_vol
                if ratio >= self.min_volume_ratio * 1.2:
                    try:
                        candle = df_5m.iloc[-(3-i)]
                    except IndexError:
                        continue
                    body = abs(candle['close'] - candle['open'])
                    tr = candle['high'] - candle['low']
                    if tr > 0 and body / tr > 0.6:
                        if trend_bias == TrendBias.BULLISH and candle['close'] > candle['open']:
                            brk.triggered = True
                            brk.breakout_direction = "BULLISH"
                            brk.breakout_price = candle['close']
                            brk.volume_ratio = ratio
                            brk.breakout_candle_volume = cvol
                            brk.volume_score = 0.5
                            return brk
                        if trend_bias == TrendBias.BEARISH and candle['close'] < candle['open']:
                            brk.triggered = True
                            brk.breakout_direction = "BEARISH"
                            brk.breakout_price = candle['close']
                            brk.volume_ratio = ratio
                            brk.breakout_candle_volume = cvol
                            brk.volume_score = 0.5
                            return brk
        except Exception as e:
            log.debug(f"vol breakout error: {e}")
        return brk
    
    def _is_flag(self, df, is_bull):
        return wave_detector._is_flag(df, is_bull)

volume_trigger = VolumeBreakoutTrigger()

# ============ LIQUIDITY POOLS (UNCHANGED) ============
def identify_liquidity_pools(df, timeframe="1h"):
    pools = {'buy_stops': [], 'sell_stops': [], 'equal_highs': [], 'equal_lows': []}
    if df is None or len(df) < 20:
        return pools
    ws = 5 if timeframe == "15m" else 3
    for i in range(ws, len(df)-ws):
        wh = df['high'].iloc[i-ws:i+ws+1]
        ch = df['high'].iloc[i]
        if ch == wh.max():
            cnt = round((wh == ch).sum())
            if cnt >= 2:
                pools['equal_highs'].append({'price': float(ch), 'timeframe': timeframe, 'candle_index': i, 'count': cnt, 'type': 'equal_high'})
                pools['sell_stops'].append({'price': float(ch), 'reason': 'equal_high', 'timeframe': timeframe, 'strength': cnt})
    for i in range(ws, len(df)-ws):
        wl = df['low'].iloc[i-ws:i+ws+1]
        cl = df['low'].iloc[i]
        if cl == wl.min():
            cnt = round((wl == cl).sum())
            if cnt >= 2:
                pools['equal_lows'].append({'price': float(cl), 'timeframe': timeframe, 'candle_index': i, 'count': cnt, 'type': 'equal_low'})
                pools['buy_stops'].append({'price': float(cl), 'reason': 'equal_low', 'timeframe': timeframe, 'strength': cnt})
    for k in pools:
        if pools[k]:
            seen = set()
            uniq = []
            for p in pools[k]:
                if p['price'] not in seen:
                    seen.add(p['price'])
                    uniq.append(p)
            pools[k] = uniq
            if k in ['buy_stops', 'equal_lows']:
                pools[k].sort(key=lambda x: x['price'])
            else:
                pools[k].sort(key=lambda x: x['price'], reverse=True)
    return pools

async def calculate_liquidity_tp_sl(exchange, symbol, side, entry_price, entry_type):
    ohlcv_4h = await fetch_ohlcv(exchange, symbol, "4h", 100)
    ohlcv_1h = await fetch_ohlcv(exchange, symbol, "1h", 200)
    ohlcv_15m = await fetch_ohlcv(exchange, symbol, "15m", 300)
    df_4h = create_dataframe(ohlcv_4h)
    df_1h = create_dataframe(ohlcv_1h)
    df_15m = create_dataframe(ohlcv_15m)
    pools_4h = identify_liquidity_pools(df_4h, "4h") if df_4h is not None else {'buy_stops': [], 'sell_stops': [], 'equal_highs': [], 'equal_lows': []}
    pools_1h = identify_liquidity_pools(df_1h, "1h") if df_1h is not None else {'buy_stops': [], 'sell_stops': [], 'equal_highs': [], 'equal_lows': []}
    pools_15m = identify_liquidity_pools(df_15m, "15m") if df_15m is not None else {'buy_stops': [], 'sell_stops': [], 'equal_highs': [], 'equal_lows': []}
    all_pools = {'buy_stops': [], 'sell_stops': [], 'equal_highs': [], 'equal_lows': []}
    for p in pools_4h['buy_stops']:
        p['weight'] = 3.0
        all_pools['buy_stops'].append(p)
    for p in pools_1h['buy_stops']:
        p['weight'] = 2.0
        all_pools['buy_stops'].append(p)
    for p in pools_15m['buy_stops']:
        p['weight'] = 1.0
        all_pools['buy_stops'].append(p)
    for pt in ['sell_stops', 'equal_highs', 'equal_lows']:
        for p in pools_4h[pt]:
            p['weight'] = 3.0
            all_pools[pt].append(p)
        for p in pools_1h[pt]:
            p['weight'] = 2.0
            all_pools[pt].append(p)
        for p in pools_15m[pt]:
            p['weight'] = 1.0
            all_pools[pt].append(p)
    all_pools['buy_stops'].sort(key=lambda x: x['price'])
    all_pools['sell_stops'].sort(key=lambda x: x['price'], reverse=True)
    all_pools['equal_highs'].sort(key=lambda x: x['price'], reverse=True)
    all_pools['equal_lows'].sort(key=lambda x: x['price'])
    cp = entry_price
    tp_targets = []
    tp_sources = []
    sl_price = 0.0
    sl_source = {}
    if side == "BUY":
        sell_stops_below = [p for p in all_pools['sell_stops'] if p['price'] < cp]
        if sell_stops_below:
            for tfw in [3.0, 2.0, 1.0]:
                tf_pools = [p for p in sell_stops_below if p.get('weight', 1.0) == tfw]
                if tf_pools:
                    best = min(tf_pools, key=lambda x: x['price'])
                    sl_price = best['price'] * 0.997
                    sl_source = {'type': 'sell_stop_pool', 'timeframe': best.get('timeframe', ''),
                                 'reason': best.get('reason', ''), 'strength': best.get('strength', 1),
                                 'original_price': best['price']}
                    break
            if sl_price == 0:
                best = min(sell_stops_below, key=lambda x: x['price'])
                sl_price = best['price'] * 0.995
        else:
            eq_lows = [p for p in all_pools['equal_lows'] if p['price'] < cp]
            if eq_lows:
                best = max(eq_lows, key=lambda x: x.get('candle_index', 0))
                sl_price = best['price'] * 0.99
            else:
                return sl_price, [], [], {}
        if sl_price > cp * 0.995:
            sl_price = cp * 0.985
        buy_stops_above = [p for p in all_pools['buy_stops'] if p['price'] > cp]
        if buy_stops_above:
            tp1 = min(buy_stops_above, key=lambda x: x['price'])
            tp_targets.append(tp1['price'])
            tp_sources.append({'tp_level': 1, 'type': 'buy_stop_pool', 'timeframe': tp1.get('timeframe', ''),
                               'reason': tp1.get('reason', ''), 'strength': tp1.get('strength', 1)})
            above_tp1 = [p for p in all_pools['buy_stops'] if p['price'] > tp_targets[0] * 1.01]
            if above_tp1:
                tp2 = min(above_tp1, key=lambda x: x['price'])
                tp_targets.append(tp2['price'])
                tp_sources.append({'tp_level': 2, 'type': 'buy_stop_pool', 'timeframe': tp2.get('timeframe', ''),
                                   'reason': 'next_pool', 'strength': tp2.get('strength', 1)})
        else:
            return sl_price, [], [], {}
    else:
        buy_stops_above = [p for p in all_pools['buy_stops'] if p['price'] > cp]
        if buy_stops_above:
            for tfw in [3.0, 2.0, 1.0]:
                tf_pools = [p for p in buy_stops_above if p.get('weight', 1.0) == tfw]
                if tf_pools:
                    best = max(tf_pools, key=lambda x: x['price'])
                    sl_price = best['price'] * 1.003
                    sl_source = {'type': 'buy_stop_pool', 'timeframe': best.get('timeframe', ''),
                                 'reason': best.get('reason', ''), 'strength': best.get('strength', 1)}
                    break
            if sl_price == 0:
                best = max(buy_stops_above, key=lambda x: x['price'])
                sl_price = best['price'] * 1.005
        else:
            eq_highs = [p for p in all_pools['equal_highs'] if p['price'] > cp]
            if eq_highs:
                best = max(eq_highs, key=lambda x: x.get('candle_index', 0))
                sl_price = best['price'] * 1.01
            else:
                return sl_price, [], [], {}
        if sl_price < cp * 1.005:
            sl_price = cp * 1.015
        sell_stops_below = [p for p in all_pools['sell_stops'] if p['price'] < cp]
        if sell_stops_below:
            tp1 = max(sell_stops_below, key=lambda x: x['price'])
            tp_targets.append(tp1['price'])
            tp_sources.append({'tp_level': 1, 'type': 'sell_stop_pool', 'timeframe': tp1.get('timeframe', ''),
                               'reason': tp1.get('reason', ''), 'strength': tp1.get('strength', 1)})
            below_tp1 = [p for p in all_pools['sell_stops'] if p['price'] < tp_targets[0] * 0.99]
            if below_tp1:
                tp2 = max(below_tp1, key=lambda x: x['price'])
                tp_targets.append(tp2['price'])
                tp_sources.append({'tp_level': 2, 'type': 'sell_stop_pool', 'timeframe': tp2.get('timeframe', ''),
                                   'reason': 'next_pool', 'strength': tp2.get('strength', 1)})
        else:
            return sl_price, [], [], {}
    risk = abs(cp - sl_price)
    reward = abs(tp_targets[0] - cp) if tp_targets else 0
    rr = reward / risk if risk > 0 else 0
    liq_analysis = {'side': side, 'entry_type': entry_type,
                    'identified_pools': {k: len(v) for k, v in all_pools.items()},
                    'sl_source': sl_source, 'tp_sources': tp_sources, 'rr_ratio': rr,
                    'risk_pct': risk / cp * 100 if cp > 0 else 0,
                    'reward_pct': reward / cp * 100 if cp > 0 and tp_targets else 0}
    return sl_price, tp_targets, tp_sources, liq_analysis

# ============ INSTITUTIONAL DATA FETCHER ============
class InstitutionalDataFetcher:
    def __init__(self):
        self.cache = {}
        self.cache_ttl = {'funding': 300, 'oi': 600, 'ticker': 30}
    async def get_institutional_data(self, exchange, symbol: str) -> InstitutionalData:
        ckey = f"{symbol}_inst"
        now = time.time()
        if ckey in self.cache:
            data, ts = self.cache[ckey]
            if now - ts < 300:
                return data
        try:
            fsym = symbol  # already swap
            tasks = [self._fetch_funding(exchange, fsym), self._fetch_oi(exchange, fsym)]
            fd, oi = await asyncio.gather(*tasks, return_exceptions=True)
            data = InstitutionalData()
            if not isinstance(fd, Exception) and fd:
                data.funding_rate = fd.get('fundingRate', 0) * 100
                data.funding_timestamp = datetime.datetime.utcnow()
            if not isinstance(oi, Exception) and oi:
                data.open_interest = oi.get('openInterest', 0)
                data.oi_timestamp = datetime.datetime.utcnow()
            self.cache[ckey] = (data, now)
            return data
        except Exception as e:
            log.warning(f"Inst data fail {symbol}: {e}")
            return InstitutionalData()
    async def _fetch_funding(self, ex, symbol):
        try:
            return await rate_limiter.execute_with_backoff(ex.fetch_funding_rate, symbol, endpoint_type="funding")
        except:
            return {}
    async def _fetch_oi(self, ex, symbol):
        try:
            return await rate_limiter.execute_with_backoff(ex.fetch_open_interest, symbol, endpoint_type="oi")
        except:
            return {}
    async def get_orderbook_bias(self, exchange, symbol: str) -> float:
        try:
            ob = await rate_limiter.execute_with_backoff(exchange.fetch_order_book, symbol, limit=20, endpoint_type="general")
            bid_vol = sum(b[1] for b in ob['bids'][:10])
            ask_vol = sum(a[1] for a in ob['asks'][:10])
            if ask_vol > 0:
                return bid_vol / ask_vol
        except:
            pass
        return 1.0

data_fetcher = InstitutionalDataFetcher()

# ============ DIRECTION ENGINE ============
class DirectionEngine:
    def __init__(self):
        self.layer_weights = {'liquidity': 0.25, 'trapped': 0.35, 'bleeding': 0.25, 'micro': 0.15}
    async def analyze_direction(self, exchange, symbol, proposed_side, current_price) -> DirectionMetrics:
        metrics = DirectionMetrics()
        try:
            inst = await data_fetcher.get_institutional_data(exchange, symbol)
            ts, tc = self._quick_trapped(inst, proposed_side, current_price)
            metrics.trapped_side = ts
            metrics.trapped_confidence = tc
            bs, fe = self._quick_bleeding(inst)
            metrics.bleeding_side = bs
            metrics.funding_extreme = fe
            ds = 0.0
            if proposed_side == "BUY":
                if ts == TrappedSide.SHORT:
                    ds += 0.3
                if bs == "LONG":
                    ds += 0.2
            else:
                if ts == TrappedSide.LONG:
                    ds += 0.3
                if bs == "SHORT":
                    ds += 0.2
            absd = abs(ds)
            if absd > 0.4:
                metrics.confidence_tier = DirectionTier.HIGH
            elif absd > 0.2:
                metrics.confidence_tier = DirectionTier.MEDIUM
            else:
                metrics.confidence_tier = DirectionTier.LOW
            metrics.direction_score = ds
            conflicts = []
            if ts == TrappedSide.LONG and proposed_side == "BUY":
                conflicts.append("Trapped LONG vs BUY")
            if ts == TrappedSide.SHORT and proposed_side == "SELL":
                conflicts.append("Trapped SHORT vs SELL")
            metrics.conflict_warnings = conflicts
        except Exception as e:
            log.debug(f"dir engine error: {e}")
        return metrics
    def _quick_trapped(self, inst, side, price):
        if inst.oi_change_24h > OI_ACCUMULATION_THRESHOLD * 100 and inst.funding_rate > FUNDING_EXTREME_THRESHOLD:
            return TrappedSide.LONG, 0.6
        if inst.oi_change_24h < -OI_ACCUMULATION_THRESHOLD * 100 and inst.funding_rate < -FUNDING_EXTREME_THRESHOLD:
            return TrappedSide.SHORT, 0.6
        return TrappedSide.NONE, 0.0
    def _quick_bleeding(self, inst):
        if inst.funding_rate > FUNDING_EXTREME_THRESHOLD:
            return "LONG", inst.funding_rate
        if inst.funding_rate < -FUNDING_EXTREME_THRESHOLD:
            return "SHORT", abs(inst.funding_rate)
        return "", 0.0

direction_engine = DirectionEngine()

# ============ FAST MOMENTUM SCALPER ============
class FastMomentumScalper:
    def __init__(self):
        self.min_vol_ratio = 1.5
        self.min_body_pct = 0.3
        self.min_rr = 2.0
        self.ema_fast = 9
        self.ema_slow = 21

    async def scan(self, exchange, symbol, current_price, trend_bias_hint=TrendBias.NEUTRAL):
        log.debug(f"Fast scalp scan start for {symbol}")
        try:
            df_3m = create_dataframe(await fetch_ohlcv(exchange, symbol, "3m", 50))
            if df_3m is None or len(df_3m) < 30:
                return None
            closes = df_3m['close'].values
            highs = df_3m['high'].values
            lows = df_3m['low'].values
            vols = df_3m['volume'].values
            opens = df_3m['open'].values
            if len(vols) >= 22:
                avg_vol = np.mean(vols[-22:-2])
            else:
                avg_vol = np.mean(vols[:-2])
            last_vol = vols[-1]
            if avg_vol <= 0 or last_vol < avg_vol * self.min_vol_ratio:
                return None
            ema9 = pd.Series(closes).ewm(span=self.ema_fast).mean().iloc[-1]
            ema21 = pd.Series(closes).ewm(span=self.ema_slow).mean().iloc[-1]
            rsi = self._rsi(closes, 14)
            body = abs(closes[-1] - opens[-1])
            wick_high = highs[-1] - max(opens[-1], closes[-1])
            wick_low = min(opens[-1], closes[-1]) - lows[-1]
            tr = highs[-1] - lows[-1]
            if tr == 0:
                return None
            body_ratio = body / tr
            side = None
            sl_price = 0.0
            tp_price = 0.0
            if ema9 > ema21 and rsi > 50 and closes[-1] > opens[-1] and body_ratio > self.min_body_pct and wick_high < body * 0.5:
                entry = highs[-1] * 1.001
                sl = lows[-1] * 0.997
                tp = current_price + (current_price - sl) * 2.0
                if tp / current_price - 1 < 0.02:
                    tp = current_price * 1.025
                if (tp - current_price) / (current_price - sl) < self.min_rr:
                    return None
                side = "BUY"
                sl_price = sl
                tp_price = tp
            elif ema9 < ema21 and rsi < 50 and closes[-1] < opens[-1] and body_ratio > self.min_body_pct and wick_low < body * 0.5:
                entry = lows[-1] * 0.999
                sl = highs[-1] * 1.003
                tp = current_price - (sl - current_price) * 2.0
                if 1 - tp / current_price < 0.02:
                    tp = current_price * 0.975
                if (current_price - tp) / (sl - current_price) < self.min_rr:
                    return None
                side = "SELL"
                sl_price = sl
                tp_price = tp
            if side is None:
                return None
            rr_ratio = abs(tp_price - current_price) / abs(current_price - sl_price) if abs(current_price - sl_price) > 0 else 0
            log.info(f"Fast scalp signal: {symbol} {side} entry={current_price:.6f} sl={sl_price:.6f} tp={tp_price:.6f}")
            return {
                "symbol": symbol,
                "timestamp": datetime.datetime.utcnow().isoformat(),
                "side": side,
                "current_price": current_price,
                "entry_price": current_price,
                "entry_type": "MOMENTUM_SCALP",
                "sl_price": sl_price,
                "tp_targets": [tp_price],
                "risk": abs(current_price - sl_price),
                "reward": abs(tp_price - current_price),
                "rr_ratio": rr_ratio,
                "trend_bias": trend_bias_hint.value,
                "quality": {"total_score": 2.2, "tier": "B"},
                "forced_move_probability": "MODERATE",
                "method": "MOMENTUM_SCALP"
            }
        except Exception as e:
            log.warning(f"fast scalp error {symbol}: {e}")
            return None

    def _rsi(self, prices, period=14):
        delta = np.diff(prices)
        gain = np.where(delta > 0, delta, 0)
        loss = np.where(delta < 0, -delta, 0)
        avg_gain = np.convolve(gain, np.ones(period) / period, mode='valid')
        avg_loss = np.convolve(loss, np.ones(period) / period, mode='valid')
        with np.errstate(divide='ignore', invalid='ignore'):
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
        return np.pad(rsi, (period, 0), constant_values=50)[-1]

fast_scalper = FastMomentumScalper()

# ============ SIGNAL TRACKER & DB & TELEGRAM ============
class SignalTracker:
    def __init__(self):
        self.active_signals = {}
        self.outcome_stats = {
            'total_signals': 0, 'tp1_hits': 0, 'tp2_hits': 0, 'tp3_hits': 0,
            'sl_hits': 0, 'expired': 0, 'active': 0, 'win_rate': 0.0, 'avg_pnl_pct': 0.0
        }
    def get_signal_key(self, setup):
        return (setup['symbol'], setup['side'], math.floor(setup.get('quality', {}).get('total_score', 0) * 2) / 2)
    def should_send_alert(self, setup):
        key = self.get_signal_key(setup)
        if key in self.active_signals and self.active_signals[key].get('status') == 'active':
            age = (datetime.datetime.utcnow() - self.active_signals[key]['first_seen']).total_seconds() / 60
            if age > SIGNAL_VALIDITY_HOURS * 60:
                log.debug(f"Signal expired for {setup['symbol']}")
                self.remove_signal_by_key(key)
                return True
            return False
        return True
    def update_signal(self, setup, alerted=False):
        key = self.get_signal_key(setup)
        now = datetime.datetime.utcnow()
        if key not in self.active_signals:
            self.active_signals[key] = {
                'setup': setup,
                'first_seen': now,
                'last_alerted': now if alerted else None,
                'last_checked': now,
                'alert_count': 1 if alerted else 0,
                'status': 'active',
                'outcome': 'active',
                'highest_price': setup.get('current_price', 0),
                'lowest_price': setup.get('current_price', 0),
                'price_at_alert': setup.get('current_price', 0) if alerted else None
            }
            self.outcome_stats['total_signals'] += 1
            self.outcome_stats['active'] += 1
            if alerted:
                log.info(f"New signal stored: {setup['symbol']} {setup['side']}")
        else:
            cp = setup.get('current_price', 0)
            self.active_signals[key]['highest_price'] = max(self.active_signals[key]['highest_price'], cp)
            self.active_signals[key]['lowest_price'] = min(self.active_signals[key]['lowest_price'], cp)
            self.active_signals[key]['last_checked'] = now
    def check_signal_outcome(self, setup, current_price):
        key = self.get_signal_key(setup)
        if key not in self.active_signals:
            return None
        sig = self.active_signals[key]
        if sig.get('status') != 'active':
            return None
        side = setup.get('side', '')
        entry = setup.get('entry_price', 0)
        tps = setup.get('tp_targets', [])
        sl = setup.get('sl_price', 0)
        if entry == 0:
            return None
        outcome = None
        for i, tp in enumerate(tps):
            if tp == 0:
                continue
            if (side == "BUY" and current_price >= tp) or (side == "SELL" and current_price <= tp):
                pnl = (current_price - entry) / entry * 100 if side == "BUY" else (entry - current_price) / entry * 100
                outcome = {'type': f'TP{i+1}_HIT', 'price': current_price, 'pnl_pct': pnl}
                break
        if not outcome and sl > 0:
            if (side == "BUY" and current_price <= sl) or (side == "SELL" and current_price >= sl):
                pnl = (current_price - entry) / entry * 100 if side == "BUY" else (entry - current_price) / entry * 100
                outcome = {'type': 'SL_HIT', 'price': current_price, 'pnl_pct': pnl}
        if outcome:
            sig['status'] = 'closed'
            sig['outcome'] = outcome['type'].lower()
            self.outcome_stats['active'] -= 1
            if 'TP1_HIT' in outcome['type']:
                self.outcome_stats['tp1_hits'] += 1
            elif 'TP2_HIT' in outcome['type']:
                self.outcome_stats['tp2_hits'] += 1
            elif 'TP3_HIT' in outcome['type']:
                self.outcome_stats['tp3_hits'] += 1
            elif outcome['type'] == 'SL_HIT':
                self.outcome_stats['sl_hits'] += 1
            wins = self.outcome_stats['tp1_hits'] + self.outcome_stats['tp2_hits'] + self.outcome_stats['tp3_hits']
            total = wins + self.outcome_stats['sl_hits']
            if total > 0:
                self.outcome_stats['win_rate'] = wins / total * 100
            log.info(f"Outcome recorded: {setup['symbol']} {outcome['type']} PnL={outcome['pnl_pct']:.2f}%")
        return outcome
    def remove_signal_by_key(self, key, reason=""):
        if key in self.active_signals:
            del self.active_signals[key]
            self.outcome_stats['active'] -= 1
            self.outcome_stats['expired'] += 1
    def cleanup_old_signals(self):
        now = datetime.datetime.utcnow()
        expired = []
        for k, v in self.active_signals.items():
            if v.get('status') == 'active' and (now - v['first_seen']).total_seconds() / 60 > SIGNAL_VALIDITY_HOURS * 60:
                expired.append(k)
        for k in expired:
            self.remove_signal_by_key(k)
        if expired:
            log.debug(f"Cleaned {len(expired)} expired signals")
    def get_stats(self):
        active = sum(1 for v in self.active_signals.values() if v.get('status') == 'active')
        return {'active_signals': active, 'outcome_stats': self.outcome_stats}

signal_tracker = SignalTracker()
db_lock = asyncio.Lock()
db_conn = None

async def send_telegram(msg: str, parse_mode="HTML"):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            await client.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": parse_mode, "disable_web_page_preview": True})
        except Exception as e:
            log.warning(f"Telegram send failed: {e}")

async def send_v6_alert(setup: Dict):
    try:
        symbol = setup['symbol']
        side = setup['side']
        quality = setup.get('quality', {})
        wave = setup.get('wave_structure', {})
        momentum = setup.get('momentum_signals', {})
        volume = setup.get('volume_breakout', {})
        direction = setup.get('direction_metrics', {})
        entry_price = setup.get('entry_price', 0)
        current_price = setup.get('current_price', 0)
        tp_targets = setup.get('tp_targets', [])
        sl_price = setup.get('sl_price', 0)
        rr_ratio = setup.get('rr_ratio', 0)
        qs = quality.get('total_score', 0)
        wave_conf = wave.pattern_confidence if isinstance(wave, WaveStructure) else wave.get('pattern_confidence', 0)
        mom_score = momentum.momentum_score if isinstance(momentum, MomentumSignals) else momentum.get('momentum_score', 0)
        vol_trig = volume.triggered if isinstance(volume, VolumeBreakout) else volume.get('triggered', False)
        if qs >= 4.5 and wave_conf >= 0.7 and mom_score >= 0.7 and vol_trig:
            tier = "S+"
            emoji = "🔮"
        elif qs >= 3.5 and mom_score >= 0.6 and vol_trig:
            tier = "A+"
            emoji = "🔥"
        elif qs >= 2.5:
            tier = "A"
            emoji = "✅"
        elif qs >= 2.0:
            tier = "B"
            emoji = "⚠️"
        else:
            tier = "C"
            emoji = "📊"
        wave_lines = []
        if isinstance(wave, WaveStructure) and wave.pattern != WavePattern.NONE:
            wave_lines.append(f"📐 Pattern: {wave.pattern.value} (Conf: {wave.pattern_confidence:.0%})")
            wave_lines.append(f"📏 Impulse: {wave.impulse_size_pct:.1f}% | Retrace: {wave.current_retracement:.0%}")
            wave_lines.append(f"🎯 Zone: {wave.fib_500:.8f} - {wave.fib_705:.8f}")
            if wave.in_optimal_zone:
                wave_lines.append("📍 IN OPTIMAL ZONE ✅")
            else:
                wave_lines.append(f"📍 Distance to zone: {wave.distance_to_zone_pct:.1f}%")
        momentum_lines = []
        if isinstance(momentum, MomentumSignals):
            if momentum.divergence_type == DivergenceType.BULLISH_REGULAR:
                div_e = "🐂"
            elif momentum.divergence_type == DivergenceType.BEARISH_REGULAR:
                div_e = "🐻"
            else:
                div_e = "⚪"
            momentum_lines.append(f"{div_e} Divergence: {momentum.divergence_type.value} ({momentum.divergence_strength:.0%})")
            if momentum.macd_crossed:
                macd_e = "✅"
            elif momentum.macd_histogram_reversal:
                macd_e = "🔄"
            else:
                macd_e = "⏳"
            momentum_lines.append(f"{macd_e} MACD Cross: {'Bullish' if momentum.macd_cross_direction == 'BULLISH' else 'Bearish' if momentum.macd_cross_direction == 'BEARISH' else 'None'}")
            momentum_lines.append(f"📊 RSI: {momentum.rsi_current:.1f} | Score: {momentum.momentum_score:.0%}")
            if momentum.momentum_aligned:
                momentum_lines.append("🎯 MOMENTUM ALIGNED ✅")
        volume_lines = []
        if isinstance(volume, VolumeBreakout):
            if volume.triggered:
                volume_lines.append(f"🚀 BREAKOUT: Volume {volume.volume_ratio:.1f}x avg")
                if volume.sweep_then_reclaim:
                    volume_lines.append("🧹 SWEEP + RECLAIM detected!")
            else:
                volume_lines.append("⏳ Waiting for volume confirmation...")
        tp_lines = []
        for i, tp in enumerate(tp_targets):
            if entry_price > 0:
                tp_lines.append(f"TP{i+1}: {tp:.8f} ({abs(tp - entry_price) / entry_price * 100:.1f}%)")
        dir_ctx = ""
        if isinstance(direction, DirectionMetrics) and direction.trapped_side != TrappedSide.NONE:
            dir_ctx += f" | Trapped: {direction.trapped_side.value}"
        msg = f"""{emoji} <b>ROMEOTPT v6.1 - {symbol} | {side}</b>
<b>Tier: {tier} | Wave-Momentum Breakout</b>

<b>📐 WAVE STRUCTURE:</b>
{chr(10).join(wave_lines) if wave_lines else 'No wave pattern'}

<b>📈 MOMENTUM:</b>
{chr(10).join(momentum_lines) if momentum_lines else 'No momentum'}

<b>📊 VOLUME:</b>
{chr(10).join(volume_lines) if volume_lines else 'No volume'}

<b>🎯 SETUP:</b>
Entry: <code>{entry_price:.8f}</code> | Now: <code>{current_price:.8f}</code>
{chr(10).join(tp_lines)}
🛡️ SL: <code>{sl_price:.8f}</code>

⚖️ RR: <b>{rr_ratio:.1f}:1</b>{dir_ctx}

🏆 Quality: {qs:.1f}/5.0 ({quality.get('tier', 'C')}) | Forced Move: {setup.get('forced_move_probability', 'LOW')}
<i>Wave+Momentum+Volume Method | {datetime.datetime.utcnow().strftime('%H:%M:%S UTC')}</i>"""
        log.info(f"📤 Alert sent: {symbol} {side} Tier={tier}")
        await send_telegram(msg)
    except Exception as e:
        log.error(f"alert formatting error: {e}")

async def send_deduped_v6_alert(setup):
    if signal_tracker.should_send_alert(setup):
        await send_v6_alert(setup)
        signal_tracker.update_signal(setup, alerted=True)
        return True
    else:
        signal_tracker.update_signal(setup, alerted=False)
        return False

# ============ DATABASE ============
async def init_database():
    global db_conn
    await db_conn.execute("""CREATE TABLE IF NOT EXISTS signals_v6_1 (
        id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, side TEXT, score REAL, timestamp TEXT,
        entry_price REAL, sl_price REAL, tp1 REAL, tp2 REAL, tp3 REAL, rr_ratio REAL,
        quality_tier TEXT, quality_score REAL, current_price REAL, trend_bias TEXT,
        wave_pattern TEXT, wave_confidence REAL, fib_retracement REAL, in_optimal_zone BOOLEAN,
        divergence_type TEXT, divergence_strength REAL, momentum_score REAL, macd_crossed BOOLEAN, momentum_aligned BOOLEAN,
        volume_triggered BOOLEAN, volume_ratio REAL, sweep_reclaim BOOLEAN,
        direction_tier TEXT, direction_score REAL, trapped_side TEXT,
        status TEXT DEFAULT 'active', alert_sent BOOLEAN DEFAULT 1,
        closed_at TEXT, closed_price REAL, outcome TEXT, pnl_pct REAL,
        UNIQUE(symbol, side, score))""")
    await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_v6_1_status ON signals_v6_1 (status)")
    await db_conn.commit()
    log.info("Database initialized/verified")

async def store_signal(setup: Dict):
    async with db_lock:
        tp = setup.get('tp_targets', [])
        q = setup.get('quality', {})
        wave = setup.get('wave_structure', {})
        mom = setup.get('momentum_signals', {})
        vol = setup.get('volume_breakout', {})
        direc = setup.get('direction_metrics', {})
        key = signal_tracker.get_signal_key(setup)
        _, _, bucket = key
        if isinstance(wave, WaveStructure):
            wp = wave.pattern.value
            wc = wave.pattern_confidence
            fr = wave.current_retracement
            iz = wave.in_optimal_zone
        else:
            wp = wave.get('pattern', 'NONE')
            wc = wave.get('pattern_confidence', 0)
            fr = wave.get('current_retracement', 0)
            iz = wave.get('in_optimal_zone', False)
        if isinstance(mom, MomentumSignals):
            dt = mom.divergence_type.value
            ds = mom.divergence_strength
            ms = mom.momentum_score
            mc = mom.macd_crossed
            ma = mom.momentum_aligned
        else:
            dt = mom.get('divergence_type', 'NONE')
            ds = mom.get('divergence_strength', 0)
            ms = mom.get('momentum_score', 0)
            mc = mom.get('macd_crossed', False)
            ma = mom.get('momentum_aligned', False)
        if isinstance(vol, VolumeBreakout):
            vt = vol.triggered
            vr = vol.volume_ratio
            sw = vol.sweep_then_reclaim
        else:
            vt = vol.get('triggered', False)
            vr = vol.get('volume_ratio', 0)
            sw = vol.get('sweep_then_reclaim', False)
        if isinstance(direc, DirectionMetrics):
            dtier = direc.confidence_tier.value
            dscore = direc.direction_score
            trap = direc.trapped_side.value
        else:
            dtier = direc.get('confidence_tier', 'LOW')
            dscore = direc.get('direction_score', 0)
            trap = direc.get('trapped_side', 'NONE')
        await db_conn.execute("""INSERT OR REPLACE INTO signals_v6_1 (
            symbol,side,score,timestamp,entry_price,sl_price,tp1,tp2,tp3,rr_ratio,quality_tier,quality_score,
            current_price,trend_bias,wave_pattern,wave_confidence,fib_retracement,in_optimal_zone,
            divergence_type,divergence_strength,momentum_score,macd_crossed,momentum_aligned,
            volume_triggered,volume_ratio,sweep_reclaim,direction_tier,direction_score,trapped_side,
            status,alert_sent) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (setup['symbol'], setup['side'], float(bucket), setup.get('timestamp', ''), float(setup.get('entry_price', 0)),
             float(setup.get('sl_price', 0)), float(tp[0]) if len(tp) > 0 else None, float(tp[1]) if len(tp) > 1 else None, float(tp[2]) if len(tp) > 2 else None,
             float(setup.get('rr_ratio', 0)), q.get('tier', 'C'), float(q.get('total_score', 0)), float(setup.get('current_price', 0)),
             setup.get('trend_bias', 'NEUTRAL'), wp, float(wc), float(fr), 1 if iz else 0,
             dt, float(ds), float(ms), 1 if mc else 0, 1 if ma else 0,
             1 if vt else 0, float(vr), 1 if sw else 0, dtier, float(dscore), trap, 'active', 1))
        await db_conn.commit()
    log.debug(f"Stored signal in DB: {setup['symbol']}")

# ============ SCANNER ============
async def scan_symbol_v6(exchange, symbol: str) -> Optional[Dict]:
    log.debug(f"Scanning {symbol} with primary method")
    try:
        df_d = create_dataframe(await fetch_ohlcv(exchange, symbol, "1d", 150))
        df_4h = create_dataframe(await fetch_ohlcv(exchange, symbol, "4h", 100))
        df_1h = create_dataframe(await fetch_ohlcv(exchange, symbol, "1h", 100))
        df_15m = create_dataframe(await fetch_ohlcv(exchange, symbol, "15m", 100))
        df_5m = create_dataframe(await fetch_ohlcv(exchange, symbol, "5m", 50))
        if df_d is None or df_4h is None or df_1h is None:
            log.debug(f"{symbol}: insufficient OHLCV data")
            return None
        ticker = await safe_fetch_ticker(exchange, symbol)
        if not ticker:
            log.debug(f"{symbol}: no ticker")
            return None
        cp = ticker.get('last', 0)
        if cp <= 0:
            log.debug(f"{symbol}: invalid price")
            return None
        trend_bias, trend_score = wave_detector.detect_trend_bias(df_d, df_4h)
        if trend_bias == TrendBias.NEUTRAL:
            log.debug(f"{symbol}: neutral trend")
            return None
        wave = wave_detector.identify_abc_correction(df_4h, trend_bias)
        if wave.pattern == WavePattern.NONE or wave.pattern_confidence < 0.3:
            log.debug(f"{symbol}: no valid wave pattern")
            return None
        momentum = momentum_engine.analyze_momentum(df_1h, df_15m, trend_bias, wave)
        if momentum.divergence_type == DivergenceType.NONE and momentum.momentum_score < 0.3:
            log.debug(f"{symbol}: insufficient momentum")
            return None
        side = "BUY" if trend_bias == TrendBias.BULLISH else "SELL"
        entry_type = "DISCOUNT_FIB_ZONE" if side == "BUY" else "PREMIUM_FIB_ZONE"
        vol_breakout = volume_trigger.detect_breakout(df_5m, df_15m, trend_bias, wave, cp)
        ob_bias = await data_fetcher.get_orderbook_bias(exchange, symbol)
        if side == "BUY" and ob_bias > 1.5:
            vol_breakout.volume_score = min(1.0, vol_breakout.volume_score + 0.1)
        elif side == "SELL" and ob_bias < 0.67:
            vol_breakout.volume_score = min(1.0, vol_breakout.volume_score + 0.1)
        sl_price, tp_targets, tp_sources, liq_analysis = await calculate_liquidity_tp_sl(exchange, symbol, side, cp, entry_type)
        if sl_price <= 0 or not tp_targets:
            log.debug(f"{symbol}: no valid TP/SL")
            return None
        risk = abs(cp - sl_price)
        reward = abs(tp_targets[0] - cp) if tp_targets else 0
        rr = reward / risk if risk > 0 else 0
        if rr < 1.5:
            log.debug(f"{symbol}: RR {rr:.1f} too low")
            return None
        dir_metrics = await direction_engine.analyze_direction(exchange, symbol, side, cp)
        qs = 0.0
        qs += wave.pattern_confidence * 1.5 + momentum.momentum_score * 1.5 + vol_breakout.volume_score * 1.0
        if wave.in_optimal_zone:
            qs += 0.5
        if dir_metrics.confidence_tier == DirectionTier.HIGH:
            qs += 0.5
        elif dir_metrics.confidence_tier == DirectionTier.MEDIUM:
            qs += 0.3
        if qs < MIN_QUALITY_SCORE:
            log.debug(f"{symbol}: quality {qs:.2f} below minimum")
            return None
        tier = "S+" if qs >= 4.5 else "A+" if qs >= 4.0 else "A" if qs >= 3.0 else "B" if qs >= 2.5 else "C"
        log.info(f"✅ Signal found: {symbol} {side} Tier={tier} Quality={qs:.2f}")
        return {
            "symbol": symbol, "timestamp": datetime.datetime.utcnow().isoformat(), "side": side, "current_price": cp,
            "entry_price": cp, "entry_type": entry_type, "sl_price": sl_price, "tp_targets": tp_targets, "tp_sources": tp_sources,
            "risk": risk, "reward": reward, "rr_ratio": rr, "trend_bias": trend_bias.value, "wave_structure": wave,
            "momentum_signals": momentum, "volume_breakout": vol_breakout, "quality": {"tier": tier, "total_score": qs},
            "liquidity_analysis": liq_analysis, "direction_metrics": dir_metrics,
            "forced_move_probability": "HIGH" if (wave.in_optimal_zone and momentum.momentum_aligned and vol_breakout.triggered and qs >= 3.5) else "MODERATE" if (momentum.momentum_aligned and qs >= 2.5) else "LOW",
            "method": "WAVE_MOMENTUM"
        }
    except Exception as e:
        log.error(f"scan error {symbol}: {e}")
        return None

async def main_scan_with_fallback(exchange, symbol):
    result = await scan_symbol_v6(exchange, symbol)
    if result:
        return result
    ticker = await safe_fetch_ticker(exchange, symbol)
    if not ticker:
        return None
    cp = ticker.get('last', 0)
    if cp <= 0:
        return None
    df_15m = create_dataframe(await fetch_ohlcv(exchange, symbol, "15m", 50))
    if df_15m is not None and len(df_15m) > 20:
        if df_15m['close'].iloc[-1] > df_15m['close'].iloc[-20:].mean():
            hint = TrendBias.BULLISH
        else:
            hint = TrendBias.BEARISH
    else:
        hint = TrendBias.NEUTRAL
    # Fallback to fast scalp
    scalp = await fast_scalper.scan(exchange, symbol, cp, hint)
    if scalp:
        try:
            scalp['direction_metrics'] = await direction_engine.analyze_direction(exchange, symbol, scalp['side'], cp)
        except:
            scalp['direction_metrics'] = DirectionMetrics()
        return scalp
    return None

class OutcomeMonitor:
    def __init__(self, exchange, interval=OUTCOME_CHECK_INTERVAL):
        self.exchange = exchange
        self.interval = interval

    async def monitor_loop(self):
        log.info("🔄 Outcome Monitor started")
        while True:
            try:
                async with db_lock:
                    cur = await db_conn.execute("SELECT symbol,side,entry_price,sl_price,tp1,tp2,tp3,id FROM signals_v6_1 WHERE status='active'")
                    rows = await cur.fetchall()
                if not rows:
                    log.debug("No active signals to monitor")
                    await asyncio.sleep(self.interval)
                    continue
                log.debug(f"Monitoring {len(rows)} active signals")
                syms = set(r[0] for r in rows)
                tickers = {}
                for s in syms:
                    t = await safe_fetch_ticker(self.exchange, s)
                    if t:
                        tickers[s] = t.get('last', 0)
                for (sym, side, entry, sl, tp1, tp2, tp3, sig_id) in rows:
                    price = tickers.get(sym)
                    if not price or price <= 0:
                        continue
                    outcome = None
                    tp_lv = 0
                    if (side == "BUY" and price <= sl) or (side == "SELL" and price >= sl):
                        outcome = "SL_HIT"
                    elif tp1 and ((side == "BUY" and price >= tp1) or (side == "SELL" and price <= tp1)):
                        outcome = "TP1_HIT"
                        tp_lv = 1
                    elif tp2 and ((side == "BUY" and price >= tp2) or (side == "SELL" and price <= tp2)):
                        outcome = "TP2_HIT"
                        tp_lv = 2
                    elif tp3 and ((side == "BUY" and price >= tp3) or (side == "SELL" and price <= tp3)):
                        outcome = "TP3_HIT"
                        tp_lv = 3
                    if outcome:
                        pnl = (price - entry) / entry * 100 if side == "BUY" else (entry - price) / entry * 100
                        em = "✅" if outcome != "SL_HIT" else "❌"
                        msg = f"{em} <b>OUTCOME ALERT</b>\n<b>{sym}</b> | {side}\n<b>{outcome}</b> at <code>{price:.8f}</code>\nProfit: <b>{pnl:+.2f}%</b>\nEntry: <code>{entry:.8f}</code> | SL: <code>{sl:.8f}</code>"
                        if tp_lv:
                            tp_price = [tp1, tp2, tp3][tp_lv - 1]
                            msg += f"\nTP{tp_lv}: <code>{tp_price:.8f}</code>"
                        log.info(f"📤 Outcome alert: {sym} {outcome} PnL={pnl:.2f}%")
                        await send_telegram(msg)
                        async with db_lock:
                            await db_conn.execute("UPDATE signals_v6_1 SET status='closed',outcome=?,closed_at=?,closed_price=?,pnl_pct=? WHERE id=?",
                                                  (outcome, datetime.datetime.utcnow().isoformat(), price, pnl, sig_id))
                            await db_conn.commit()
            except Exception as e:
                log.error(f"Outcome monitor error: {e}")
            await asyncio.sleep(self.interval)

async def v6_scanner_main(exchange):
    startup_msg = f"🚀 <b>ROMEOTPT v6.1 – MEXC FUTURES</b>\nScan: {SCAN_INTERVAL}s | Top {TOP_N} | Quality ≥{MIN_QUALITY_SCORE}\n<b>Primary Method: Wave + Momentum + Volume</b> + Fast Scalp + Outcome Alerts"
    log.info("Scanner starting...")
    await send_telegram(startup_msg)
    scan_cycle = 0
    while True:
        scan_cycle += 1
        cycle_start = time.time()
        log.info(f"🔄 Scan cycle #{scan_cycle} started")
        try:
            tickers = await safe_fetch_tickers(exchange)
            usdt_pairs = []
            for sym, data in tickers.items():
                if sym.endswith("USDT:USDT") and not sym.startswith("USDT"):
                    vol = data.get('quoteVolume')
                    if vol is None or vol == 0:
                        base = data.get('baseVolume', 0)
                        last = data.get('last', 0)
                        if base and last:
                            vol = base * last
                    if isinstance(vol, (int, float)) and vol > 300000:
                        usdt_pairs.append((sym, float(vol)))
            usdt_pairs.sort(key=lambda x: x[1], reverse=True)
            symbols = [s[0] for s in usdt_pairs[:TOP_N]]
            stats = signal_tracker.get_stats()
            log.info(f"Scanning top {len(symbols)} symbols (active signals: {stats['active_signals']})")
            alerts_this = 0
            tasks = []
            for sym in symbols:
                tasks.append(asyncio.create_task(main_scan_with_fallback(exchange, sym)))
                if len(tasks) >= 5:
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    for r in results:
                        if isinstance(r, Exception):
                            log.warning(f"Task exception: {r}")
                            continue
                        if r:
                            if await send_deduped_v6_alert(r):
                                alerts_this += 1
                            await store_signal(r)
                    tasks = []
                    await asyncio.sleep(0.5)
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for r in results:
                    if isinstance(r, Exception):
                        continue
                    if r:
                        if await send_deduped_v6_alert(r):
                            alerts_this += 1
                        await store_signal(r)
            signal_tracker.cleanup_old_signals()
            cycle_duration = time.time() - cycle_start
            log.info(f"Cycle #{scan_cycle} completed in {cycle_duration:.1f}s, alerts sent: {alerts_this}")
            if scan_cycle % 5 == 0:
                os_stat = stats.get('outcome_stats', {})
                wins = os_stat.get('tp1_hits', 0) + os_stat.get('tp2_hits', 0) + os_stat.get('tp3_hits', 0)
                losses = os_stat.get('sl_hits', 0)
                if wins + losses > 0:
                    log.info(f"📈 Cumulative WR={os_stat.get('win_rate', 0):.1f}% Total={wins+losses}")
            await asyncio.sleep(max(0, SCAN_INTERVAL - cycle_duration))
        except Exception as e:
            log.error(f"Fatal scanner loop error: {e}", exc_info=True)
            await asyncio.sleep(SCAN_INTERVAL * 2)

# ============ FASTAPI ============
app = FastAPI()

@app.get("/health")
async def health():
    stats = signal_tracker.get_stats()
    return {
        "status": "healthy",
        "version": "6.1 MEXC Futures",
        "active_signals": stats['active_signals'],
        "outcome_stats": stats['outcome_stats']
    }

@app.get("/signals/active")
async def get_active():
    act = []
    for k, v in signal_tracker.active_signals.items():
        if v.get('status') == 'active':
            sym, side, bucket = k
            st = v.get('setup', {})
            tp_list = st.get('tp_targets', [])
            act.append({
                "symbol": sym,
                "side": side,
                "quality_score": st.get('quality', {}).get('total_score', 0),
                "entry": st.get('entry_price', 0),
                "sl": st.get('sl_price', 0),
                "tp1": tp_list[0] if len(tp_list) > 0 else 0,
                "rr": st.get('rr_ratio', 0)
            })
    return {"active_signals": act, "count": len(act)}

# ============ MAIN ============
async def main():
    global db_conn
    try:
        db_conn = await aiosqlite.connect(DB_PATH)
        await init_database()
        exchange = ccxt.mexc({
            'enableRateLimit': True,
            'options': {'defaultType': 'swap', 'fetchFundingRateHistory': True, 'fetchOpenInterest': True},
            'rateLimit': 50,
            'timeout': 30000,
            'verbose': False,
        })
        log.info("🚀 ROMEOTPT v6.1 MEXC Futures starting")
        monitor = OutcomeMonitor(exchange)
        asyncio.create_task(monitor.monitor_loop())
        await v6_scanner_main(exchange)
    except Exception as e:
        log.critical(f"Fatal startup error: {e}", exc_info=True)
    finally:
        if db_conn:
            await db_conn.close()
        log.info("Shutdown complete")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--http", action="store_true")
    args = parser.parse_args()
    if args.http:
        uvicorn.run(app, host="0.0.0.0", port=8000)
    else:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            log.info("Stopped by user")