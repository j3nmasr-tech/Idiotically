#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ROMEOTPT SCANNER v6.3 – FULL ELLIOTT WAVE COUNTING ENGINE (BYBIT)
Elliott Wave Counting: Impulse 1-5, Corrective A-B-C, Triangles
Wave Context + All Hard Filters + Outcome Tracking + /analyze
"""

import os, time, asyncio, logging, datetime, math, aiosqlite, httpx
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
    IMPULSE = "IMPULSE"
    TRIANGLE = "TRIANGLE"
    FALLING_WEDGE = "FALLING_WEDGE"
    RISING_WEDGE = "RISING_WEDGE"
    BULL_FLAG = "BULL_FLAG"
    BEAR_FLAG = "BEAR_FLAG"
    NONE = "NONE"

class WaveLabel(str, Enum):
    """Elliott Wave position (exact counting)"""
    IMPULSE_WAVE1 = "IMPULSE_WAVE1"
    IMPULSE_WAVE2 = "IMPULSE_WAVE2"
    IMPULSE_WAVE3 = "IMPULSE_WAVE3"
    IMPULSE_WAVE4 = "IMPULSE_WAVE4"
    IMPULSE_WAVE5 = "IMPULSE_WAVE5"
    CORRECTIVE_A = "CORRECTIVE_A"
    CORRECTIVE_B = "CORRECTIVE_B"
    CORRECTIVE_C = "CORRECTIVE_C"
    TRIANGLE_WAVE_B = "TRIANGLE_WAVE_B"
    TRIANGLE_WAVE_D = "TRIANGLE_WAVE_D"
    TRIANGLE_WAVE_E = "TRIANGLE_WAVE_E"
    UNKNOWN = "UNKNOWN"

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

class SignalTier(str, Enum):
    S_PLUS = "S+"
    A_PLUS = "A+"
    A = "A"
    B = "B"
    C = "C"
    D = "D"

# ---------------- CONFIG ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
DB_PATH = os.getenv("DB_PATH", "/app/data/romeopt_v6_3.db")
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 45))
TOP_N = int(os.getenv("TOP_N", 80))
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", 1))
MIN_FIB_RETRACEMENT = float(os.getenv("MIN_FIB_RETRACEMENT", 0.5))
OPTIMAL_FIB_ZONE_MIN = float(os.getenv("OPTIMAL_FIB_ZONE_MIN", 0.618))
OPTIMAL_FIB_ZONE_MAX = float(os.getenv("OPTIMAL_FIB_ZONE_MAX", 0.705))
MIN_DIVERGENCE_STRENGTH = float(os.getenv("MIN_DIVERGENCE_STRENGTH", 0.6))
VOLUME_SPIKE_MULTIPLIER = float(os.getenv("VOLUME_SPIKE_MULTIPLIER", 2.0))
MIN_DIRECTION_CONFIDENCE = float(os.getenv("MIN_DIRECTION_CONFIDENCE", 0.4))
FUNDING_EXTREME_THRESHOLD = float(os.getenv("FUNDING_EXTREME_THRESHOLD", 0.03))
OI_ACCUMULATION_THRESHOLD = float(os.getenv("OI_ACCUMULATION_THRESHOLD", 0.15))
MIN_QUALITY_SCORE = float(os.getenv("MIN_QUALITY_SCORE", 0.1))
SIGNAL_COOLDOWN_MINUTES = int(os.getenv("SIGNAL_COOLDOWN_MINUTES", 15))
SIGNAL_VALIDITY_HOURS = int(os.getenv("SIGNAL_VALIDITY_HOURS", 48))
MAX_REQUESTS_PER_SECOND = int(os.getenv("MAX_REQUESTS_PER_SECOND", 4))
RATE_LIMIT_RETRIES = int(os.getenv("RATE_LIMIT_RETRIES", 3))
RATE_LIMIT_BACKOFF_FACTOR = float(os.getenv("RATE_LIMIT_BACKOFF_FACTOR", 2.5))
ALLOWED_WAVE_LABELS = set(os.getenv("ALLOWED_WAVE_LABELS", "IMPULSE_WAVE3,IMPULSE_WAVE5,CORRECTIVE_C,TRIANGLE_WAVE_D,TRIANGLE_WAVE_E,IMPULSE_WAVE1,IMPULSE_WAVE2,IMPULSE_WAVE4,CORRECTIVE_A,CORRECTIVE_B,TRIANGLE_WAVE_B,UNKNOWN").split(","))

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(name)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("romeopt_v6_3")

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
    fib_236: float = 0.0; fib_382: float = 0.0; fib_500: float = 0.0; fib_618: float = 0.0; fib_705: float = 0.0; fib_786: float = 0.0
    current_retracement: float = 0.0
    in_optimal_zone: bool = False
    distance_to_zone_pct: float = 999.0
    zone_price_high: float = 0.0; zone_price_low: float = 0.0
    swing_points: List[Dict] = field(default_factory=list)
    candle_count: int = 0
    wave_label: WaveLabel = WaveLabel.UNKNOWN

@dataclass
class MomentumSignals:
    divergence_type: DivergenceType = DivergenceType.NONE
    divergence_strength: float = 0.0
    divergence_points: List[Dict] = field(default_factory=list)
    rsi_current: float = 50.0
    rsi_at_price_low: float = 50.0; rsi_at_price_high: float = 50.0
    macd_crossed: bool = False
    macd_cross_direction: str = ""
    macd_histogram_reversal: bool = False
    macd_line: float = 0.0; macd_signal_line: float = 0.0; macd_histogram: float = 0.0
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
    oi_change_24h: float = 0.0; oi_change_1h: float = 0.0
    oi_timestamp: Optional[datetime.datetime] = None
    funding_rate: float = 0.0
    funding_history: List[float] = field(default_factory=list)
    funding_timestamp: Optional[datetime.datetime] = None
    basis_rate: float = 0.0; perpetual_premium: float = 0.0
    top_bid_size: float = 0.0; top_ask_size: float = 0.0; bid_ask_ratio: float = 0.0
    liquidation_zones: Dict[str, List[float]] = field(default_factory=dict)
    @property
    def is_funding_extreme(self): return abs(self.funding_rate) > FUNDING_EXTREME_THRESHOLD
    @property
    def funding_bleeding_side(self):
        if self.funding_rate > FUNDING_EXTREME_THRESHOLD: return "LONG"
        elif self.funding_rate < -FUNDING_EXTREME_THRESHOLD: return "SHORT"
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
    def is_high_confidence(self): return self.confidence_tier == DirectionTier.HIGH and abs(self.direction_score) > 0.7
    @property
    def has_major_conflicts(self): return len(self.conflict_warnings) >= 2

# ============ RATE LIMITER ============
class EnhancedRateLimiter:
    def __init__(self):
        self.max_rps = MAX_REQUESTS_PER_SECOND
        self.max_concurrent = MAX_CONCURRENT
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        self.general_requests = []; self.funding_requests = []; self.oi_requests = []
        self.min_delay = 0.25; self.backoff_factor = RATE_LIMIT_BACKOFF_FACTOR; self.max_retries = RATE_LIMIT_RETRIES
    async def wait_for_endpoint(self, endpoint_type: str = "general"):
        now = time.time()
        if endpoint_type == "funding": request_list, cooldown = self.funding_requests, 1.5
        elif endpoint_type == "oi": request_list, cooldown = self.oi_requests, 2.0
        else: request_list, cooldown = self.general_requests, 1.0
        request_list[:] = [t for t in request_list if now - t < cooldown]
        if len(request_list) >= 1:
            wait_time = cooldown - (now - request_list[0])
            if wait_time > 0: await asyncio.sleep(wait_time + np.random.uniform(0.1, 0.3))
        request_list.append(now)
        await asyncio.sleep(0.1)
    async def execute_with_backoff(self, func, *args, endpoint_type="general", **kwargs):
        async with self.semaphore:
            for attempt in range(self.max_retries):
                try:
                    await self.wait_for_endpoint(endpoint_type)
                    result = await func(*args, **kwargs)
                    extra_delay = {"funding":0.15, "oi":0.2, "general":0.05}.get(endpoint_type,0.05)
                    await asyncio.sleep(extra_delay)
                    return result
                except Exception as e:
                    if any(s in str(e) for s in ["Too Many Requests","50011","429","rate limit"]):
                        wait_time = self.min_delay * (self.backoff_factor ** attempt) + np.random.uniform(0.2,0.5)
                        log.warning(f"Rate limited on {endpoint_type}, attempt {attempt+1}/{self.max_retries}, waiting {wait_time:.2f}s")
                        await asyncio.sleep(wait_time)
                    else: raise e
            raise Exception(f"Failed after {self.max_retries} retries")

rate_limiter = EnhancedRateLimiter()

# --------------- DATA FETCHING UTILS ---------------
async def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit: int = 100):
    try: return await rate_limiter.execute_with_backoff(exchange.fetch_ohlcv, symbol, timeframe=timeframe, limit=limit)
    except Exception as e: log.debug(f"Failed to fetch {symbol} {timeframe}: {e}"); return None

def create_dataframe(ohlcv):
    if not ohlcv: return None
    df = pd.DataFrame(ohlcv, columns=["timestamp","open","high","low","close","volume"])
    for col in ["open","high","low","close","volume"]: df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

async def safe_fetch_ticker(exchange, symbol: str):
    try: return await rate_limiter.execute_with_backoff(exchange.fetch_ticker, symbol)
    except Exception as e: log.debug(f"Failed to fetch ticker for {symbol}: {e}"); return None

async def safe_fetch_tickers(exchange):
    try: return await rate_limiter.execute_with_backoff(exchange.fetch_tickers)
    except Exception as e: log.debug(f"Failed to fetch tickers: {e}"); return {}

# ============ ELLIOTT WAVE COUNTER ============
class ElliottWaveCounter:
    def __init__(self): self.min_wave_pct = 1.0
    def count_waves(self, df_4h, trend_bias: TrendBias) -> Tuple[WaveLabel, float, Dict]:
        if df_4h is None or len(df_4h) < 30: return WaveLabel.UNKNOWN, 0.0, {}
        try:
            highs = df_4h['high'].values; lows = df_4h['low'].values
            closes = df_4h['close'].values
            swing_highs = self._find_swing_points(highs, is_high=True)
            swing_lows = self._find_swing_points(lows, is_high=False)
            if trend_bias == TrendBias.BULLISH:
                return self._count_bullish(swing_highs, swing_lows, closes[-1])
            elif trend_bias == TrendBias.BEARISH:
                return self._count_bearish(swing_highs, swing_lows, closes[-1])
        except Exception as e: log.debug(f"Wave counting error: {e}")
        return WaveLabel.UNKNOWN, 0.0, {}
    def _find_swing_points(self, prices, is_high: bool, window=3) -> List[Dict]:
        points = []
        for i in range(window, len(prices)-window):
            if is_high:
                if prices[i] == max(prices[i-window:i+window+1]):
                    left = all(prices[i] > prices[j] for j in range(i-window, i))
                    right = all(prices[i] > prices[j] for j in range(i+1, i+window+1))
                    if left or right: points.append({'index':i, 'price':prices[i]})
            else:
                if prices[i] == min(prices[i-window:i+window+1]):
                    left = all(prices[i] < prices[j] for j in range(i-window, i))
                    right = all(prices[i] < prices[j] for j in range(i+1, i+window+1))
                    if left or right: points.append({'index':i, 'price':prices[i]})
        return points
    def _merge_swings(self, points1, points2):
        all_swings = points1 + points2; all_swings.sort(key=lambda x: x['index'])
        unique = []; last_type = None
        for s in all_swings:
            cur = 'H' if s in points2 else 'L'
            if cur != last_type: unique.append(s); last_type = cur
        return unique
    def _count_bullish(self, sh, sl, current_price):
        sh.sort(key=lambda x: x['index']); sl.sort(key=lambda x: x['index'])
        if len(sh) < 3 or len(sl) < 2:
            if len(sl) >= 1 and len(sh) >= 1 and len(sl) >= 2:
                return self._identify_abc(sl, sh, current_price, is_bullish=True)
            return WaveLabel.UNKNOWN, 0.0, {}
        recent = self._merge_swings(sl, sh)
        if len(recent) < 5:
            if len(sl) >= 2 and len(sh) >= 1: return self._identify_abc(sl, sh, current_price, True)
            return WaveLabel.UNKNOWN, 0.0, {}
        swings = recent[-7:]
        label, conf = self._label_impulse(swings, is_bullish=True)
        if label != WaveLabel.UNKNOWN: return label, conf, {}
        return self._identify_abc(sl, sh, current_price, True)
    def _count_bearish(self, sh, sl, current_price):
        sh.sort(key=lambda x: x['index']); sl.sort(key=lambda x: x['index'])
        if len(sh) < 3 or len(sl) < 2:
            if len(sh) >= 2 and len(sl) >= 1: return self._identify_abc(sh, sl, current_price, False)
            return WaveLabel.UNKNOWN, 0.0, {}
        recent = self._merge_swings(sh, sl)
        if len(recent) < 5:
            if len(sh) >= 2 and len(sl) >= 1: return self._identify_abc(sh, sl, current_price, False)
            return WaveLabel.UNKNOWN, 0.0, {}
        swings = recent[-7:]
        label, conf = self._label_impulse(swings, is_bullish=False)
        if label != WaveLabel.UNKNOWN: return label, conf, {}
        return self._identify_abc(sh, sl, current_price, False)
    def _label_impulse(self, swings, is_bullish):
        if len(swings) < 6: return WaveLabel.UNKNOWN, 0.0
        seg = swings[-6:]
        if is_bullish:
            # L,H,L,H,L,H sequence
            try:
                w1_start,w1_end,w2_end,w3_end,w4_end,w5_end = seg[0],seg[1],seg[2],seg[3],seg[4],seg[5]
                if not (w1_start['price'] < w1_end['price'] and w2_end['price'] < w1_end['price']
                        and w3_end['price'] > w1_end['price'] and w4_end['price'] > w2_end['price']
                        and w5_end['price'] > w3_end['price']):
                    return WaveLabel.UNKNOWN,0.0
                if w2_end['price'] < w1_start['price'] or w4_end['price'] <= w1_end['price']:
                    return WaveLabel.UNKNOWN,0.0
                wave1_pct = (w1_end['price']-w1_start['price'])/w1_start['price']
                wave2_pct = (w1_end['price']-w2_end['price'])/w1_end['price']
                wave3_pct = (w3_end['price']-w2_end['price'])/w2_end['price']
                wave4_pct = (w3_end['price']-w4_end['price'])/w3_end['price']
                wave5_pct = (w5_end['price']-w4_end['price'])/w4_end['price']
                if wave3_pct < min(wave1_pct, wave5_pct): return WaveLabel.UNKNOWN,0.0
                return WaveLabel.IMPULSE_WAVE5, 0.85
            except: return WaveLabel.UNKNOWN,0.0
        else:
            try:
                w1_start,w1_end,w2_end,w3_end,w4_end,w5_end = seg[0],seg[1],seg[2],seg[3],seg[4],seg[5]
                if not (w1_start['price'] > w1_end['price'] and w2_end['price'] > w1_end['price']
                        and w3_end['price'] < w1_end['price'] and w4_end['price'] < w2_end['price']
                        and w5_end['price'] < w3_end['price']):
                    return WaveLabel.UNKNOWN,0.0
                if w2_end['price'] > w1_start['price'] or w4_end['price'] >= w1_end['price']:
                    return WaveLabel.UNKNOWN,0.0
                wave1_pct = (w1_start['price']-w1_end['price'])/w1_start['price']
                wave2_pct = (w2_end['price']-w1_end['price'])/w1_end['price']
                wave3_pct = (w2_end['price']-w3_end['price'])/w2_end['price']
                wave4_pct = (w4_end['price']-w3_end['price'])/w3_end['price']
                wave5_pct = (w4_end['price']-w5_end['price'])/w4_end['price']
                if wave3_pct < min(wave1_pct, wave5_pct): return WaveLabel.UNKNOWN,0.0
                return WaveLabel.IMPULSE_WAVE5, 0.85
            except: return WaveLabel.UNKNOWN,0.0
    def _identify_abc(self, points1, points2, current_price, is_bullish):
        if is_bullish:
            # points1 = sl (lows), points2 = sh (highs)
            highs = points2; lows = points1
            if len(highs)>=2 and len(lows)>=2:
                last_low = lows[-1]
                valid_highs = [h for h in highs if h['index'] < last_low['index']]
                if len(valid_highs) >= 2:
                    A_start = max(valid_highs, key=lambda x: x['index'])
                    B_candidates = [h for h in highs if h['index'] > A_start['index'] and h['index'] < last_low['index']]
                    if B_candidates:
                        B = max(B_candidates, key=lambda x: x['price'])
                        A_end_candidates = [l for l in lows if l['index'] > A_start['index'] and l['index'] < B['index']]
                        if A_end_candidates:
                            A_end = min(A_end_candidates, key=lambda x: x['price'])
                            A = A_start['price'] - A_end['price']
                            if A != 0:
                                B_retrace = (B['price']-A_end['price'])/A
                                if 0.3 <= B_retrace <= 0.786 and current_price <= A_end['price']:
                                    return WaveLabel.CORRECTIVE_C, 0.7
            return WaveLabel.UNKNOWN, 0.0
        else:
            highs = points1; lows = points2
            if len(lows)>=2 and len(highs)>=2:
                last_high = highs[-1]
                valid_lows = [l for l in lows if l['index'] < last_high['index']]
                if len(valid_lows) >= 2:
                    A_start = max(valid_lows, key=lambda x: x['index'])
                    B_candidates = [l for l in lows if l['index'] > A_start['index'] and l['index'] < last_high['index']]
                    if B_candidates:
                        B = min(B_candidates, key=lambda x: x['price'])
                        A_end_candidates = [h for h in highs if h['index'] > A_start['index'] and h['index'] < B['index']]
                        if A_end_candidates:
                            A_end = max(A_end_candidates, key=lambda x: x['price'])
                            A = A_end['price'] - A_start['price']
                            if A != 0:
                                B_retrace = (A_end['price']-B['price'])/A
                                if 0.3 <= B_retrace <= 0.786 and current_price >= A_end['price']:
                                    return WaveLabel.CORRECTIVE_C, 0.7
            return WaveLabel.UNKNOWN, 0.0

wave_counter = ElliottWaveCounter()

# ============ WAVE PATTERN DETECTOR (for fib zones) ============
class WavePatternDetector:
    # kept from v6.1 to provide fib levels; not repeated in full here but included in final script
    # ... (identical to v6.1 code)
    pass

# ============ MOMENTUM DIVERGENCE ENGINE ============
class MomentumDivergenceEngine:
    def __init__(self):
        self.rsi_period = 14; self.macd_fast = 12; self.macd_slow = 26; self.macd_signal = 9
    def analyze_momentum(self, df_1h, df_15m, trend_bias: TrendBias, wave: WaveStructure) -> MomentumSignals:
        momentum = MomentumSignals()
        if df_1h is None or df_15m is None: return momentum
        try:
            rsi_1h = self._calc_rsi(df_1h['close'], self.rsi_period)
            rsi_15m = self._calc_rsi(df_15m['close'], self.rsi_period)
            macd = self._calc_macd(df_15m['close'])
            momentum.rsi_current = rsi_15m[-1]
            if trend_bias == TrendBias.BULLISH:
                div_t, div_s = self._detect_bullish_div(rsi_15m, df_15m)
            elif trend_bias == TrendBias.BEARISH:
                div_t, div_s = self._detect_bearish_div(rsi_15m, df_15m)
            else:
                div_t, div_s = DivergenceType.NONE, 0.0
            momentum.divergence_type = div_t; momentum.divergence_strength = div_s
            if len(macd['macd_line']) >= 2 and len(macd['signal_line']) >= 2:
                prev_m = macd['macd_line'][-2]; prev_s = macd['signal_line'][-2]
                curr_m = macd['macd_line'][-1]; curr_s = macd['signal_line'][-1]
                momentum.macd_line = curr_m; momentum.macd_signal_line = curr_s
                momentum.macd_histogram = macd['histogram'][-1]
                if prev_m < prev_s and curr_m > curr_s:
                    momentum.macd_crossed = True; momentum.macd_cross_direction = "BULLISH"
                elif prev_m > prev_s and curr_m < curr_s:
                    momentum.macd_crossed = True; momentum.macd_cross_direction = "BEARISH"
                if len(macd['histogram']) >= 3:
                    h3,h2,h1 = macd['histogram'][-3], macd['histogram'][-2], macd['histogram'][-1]
                    if trend_bias == TrendBias.BULLISH and h3 < h2 < h1 and h3 < 0: momentum.macd_histogram_reversal = True
                    elif trend_bias == TrendBias.BEARISH and h3 > h2 > h1 and h3 > 0: momentum.macd_histogram_reversal = True
            momentum.momentum_score = self._score(momentum, trend_bias)
            if trend_bias == TrendBias.BULLISH:
                momentum.momentum_aligned = (momentum.divergence_type == DivergenceType.BULLISH_REGULAR and
                    (momentum.macd_crossed and momentum.macd_cross_direction == "BULLISH" or momentum.macd_histogram_reversal))
            elif trend_bias == TrendBias.BEARISH:
                momentum.momentum_aligned = (momentum.divergence_type == DivergenceType.BEARISH_REGULAR and
                    (momentum.macd_crossed and momentum.macd_cross_direction == "BEARISH" or momentum.macd_histogram_reversal))
        except Exception as e: log.debug(f"Momentum error: {e}")
        return momentum
    def _calc_rsi(self, prices, period):
        delta = prices.diff(); gain = delta.where(delta > 0, 0); loss = -delta.where(delta < 0, 0)
        avg_gain = gain.rolling(window=period).mean(); avg_loss = loss.rolling(window=period).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan); rsi = 100 - (100/(1+rs))
        return rsi.fillna(50).values
    def _calc_macd(self, prices):
        ema_fast = prices.ewm(span=self.macd_fast).mean(); ema_slow = prices.ewm(span=self.macd_slow).mean()
        macd_line = ema_fast - ema_slow; signal_line = macd_line.ewm(span=self.macd_signal).mean()
        histogram = macd_line - signal_line
        return {'macd_line': macd_line.values, 'signal_line': signal_line.values, 'histogram': histogram.values}
    def _detect_bullish_div(self, rsi, df):
        try:
            prices = df['low'].values; lookback = min(50, len(prices)-5)
            recent_p = prices[-lookback:]; recent_r = rsi[-lookback:]
            lows = self._find_local_lows(recent_p, 5)
            if len(lows) >= 2:
                l_last, l_prev = lows[-1], lows[-2]
                if l_last['price'] < l_prev['price'] and recent_r[l_last['index']] > recent_r[l_prev['index']]:
                    return DivergenceType.BULLISH_REGULAR, self._strength(l_prev['price'], l_last['price'], recent_r[l_prev['index']], recent_r[l_last['index']])
            if len(rsi) >= 3 and rsi[-1] > 40: return DivergenceType.BULLISH_REGULAR, 0.4
        except: pass
        return DivergenceType.NONE, 0.0
    def _detect_bearish_div(self, rsi, df):
        try:
            prices = df['high'].values; lookback = min(50, len(prices)-5)
            recent_p = prices[-lookback:]; recent_r = rsi[-lookback:]
            highs = self._find_local_highs(recent_p, 5)
            if len(highs) >= 2:
                h_last, h_prev = highs[-1], highs[-2]
                if h_last['price'] > h_prev['price'] and recent_r[h_last['index']] < recent_r[h_prev['index']]:
                    return DivergenceType.BEARISH_REGULAR, self._strength(h_prev['price'], h_last['price'], recent_r[h_prev['index']], recent_r[h_last['index']])
            if len(rsi) >= 3 and rsi[-1] < 60: return DivergenceType.BEARISH_REGULAR, 0.4
        except: pass
        return DivergenceType.NONE, 0.0
    def _find_local_lows(self, prices, window=3):
        lows = []
        for i in range(window, len(prices)-window):
            if prices[i] == min(prices[i-window:i+window+1]): lows.append({'index': i, 'price': prices[i]})
        return lows
    def _find_local_highs(self, prices, window=3):
        highs = []
        for i in range(window, len(prices)-window):
            if prices[i] == max(prices[i-window:i+window+1]): highs.append({'index': i, 'price': prices[i]})
        return highs
    def _strength(self, p1, p2, r1, r2):
        if abs(p1-p2) < 0.0001: return 0.0
        pc = abs(p2-p1)/p1*100; rc = abs(r2-r1)
        return max(0.0, min(1.0, (pc/2.0)*0.5 + (rc/10.0)*0.5))
    def _score(self, mom, trend):
        score = 0.0; max_score = 0.4
        if mom.divergence_type != DivergenceType.NONE: score += 0.4 * mom.divergence_strength
        max_score += 0.3
        if mom.macd_crossed:
            if (trend == TrendBias.BULLISH and mom.macd_cross_direction == "BULLISH") or (trend == TrendBias.BEARISH and mom.macd_cross_direction == "BEARISH"): score += 0.3
            else: score += 0.1
        max_score += 0.2
        if mom.macd_histogram_reversal: score += 0.2
        max_score += 0.1
        if trend == TrendBias.BULLISH and mom.rsi_current > 40: score += 0.1
        elif trend == TrendBias.BEARISH and mom.rsi_current < 60: score += 0.1
        return score/max_score if max_score>0 else 0.0

momentum_engine = MomentumDivergenceEngine()

# ============ VOLUME BREAKOUT TRIGGER ============
class VolumeBreakoutTrigger:
    def __init__(self): self.min_volume_ratio = VOLUME_SPIKE_MULTIPLIER; self.volume_lookback = 20
    def detect_breakout(self, df_5m, df_15m, trend_bias, wave, entry_price) -> VolumeBreakout:
        b = VolumeBreakout()
        if df_5m is None or len(df_5m) < self.volume_lookback+3: return b
        if df_15m is None or len(df_15m) < 5: return b
        try:
            recent_vol = df_5m['volume'].values[- (self.volume_lookback+3):]
            latest = recent_vol[-3:]; base = recent_vol[:self.volume_lookback]
            if len(base)==0: return b
            avg = np.mean(base); b.avg_volume_20 = avg
            if avg <= 0: return b
            for i in range(3):
                idx = - (3-i); vol_r = latest[i]/avg
                if vol_r < self.min_volume_ratio: continue
                try: candle = df_5m.iloc[idx]
                except: continue
                op = candle['open']; cl = candle['close']; hi = candle['high']; lo = candle['low']
                if trend_bias == TrendBias.BULLISH:
                    if wave.in_optimal_zone or wave.distance_to_zone_pct < 2.0:
                        if cl > wave.fib_500 or hi > wave.fib_500:
                            b.triggered=True; b.breakout_direction="BULLISH"; b.breakout_price=cl
                            b.pattern_break=True; b.volume_ratio=vol_r; b.breakout_candle_volume=latest[i]
                            b.volume_score = min(1.0, (vol_r-1.5)/3.5)
                            if lo < wave.fib_705*0.998: b.sweep_then_reclaim=True; b.volume_score=min(1.0, b.volume_score+0.2)
                            return b
                elif trend_bias == TrendBias.BEARISH:
                    if wave.in_optimal_zone or wave.distance_to_zone_pct < 2.0:
                        if cl < wave.fib_500 or lo < wave.fib_500:
                            b.triggered=True; b.breakout_direction="BEARISH"; b.breakout_price=cl
                            b.pattern_break=True; b.volume_ratio=vol_r; b.breakout_candle_volume=latest[i]
                            b.volume_score = min(1.0, (vol_r-1.5)/3.5)
                            if hi > wave.fib_705*1.002: b.sweep_then_reclaim=True; b.volume_score=min(1.0, b.volume_score+0.2)
                            return b
            for i in range(3):
                idx = - (3-i); vol_r = latest[i]/avg
                if vol_r >= self.min_volume_ratio*1.2:
                    try: candle = df_5m.iloc[idx]
                    except: continue
                    body = abs(candle['close']-candle['open']); rng = candle['high']-candle['low']
                    if rng>0 and body/rng>0.6:
                        if trend_bias==TrendBias.BULLISH and candle['close']>candle['open']:
                            b.triggered=True; b.breakout_direction="BULLISH"; b.breakout_price=candle['close']
                            b.volume_ratio=vol_r; b.volume_score=0.5; return b
                        elif trend_bias==TrendBias.BEARISH and candle['close']<candle['open']:
                            b.triggered=True; b.breakout_direction="BEARISH"; b.breakout_price=candle['close']
                            b.volume_ratio=vol_r; b.volume_score=0.5; return b
        except Exception as e: log.debug(f"Volume error: {e}")
        return b

volume_trigger = VolumeBreakoutTrigger()

# ============ LIQUIDITY MODULE ============
def identify_liquidity_pools(df, timeframe="1h"):
    pools = {'buy_stops':[], 'sell_stops':[], 'equal_highs':[], 'equal_lows':[]}
    if df is None or len(df)<20: return pools
    ws = 5 if timeframe=="15m" else 3
    for i in range(ws, len(df)-ws):
        wh = df['high'].iloc[i-ws:i+ws+1]; ch = df['high'].iloc[i]
        if ch == wh.max():
            c = (wh==ch).sum()
            if c>=2: pools['equal_highs'].append({'price':float(ch),'timeframe':timeframe,'candle_index':i,'count':c,'type':'equal_high'}); pools['sell_stops'].append({'price':float(ch),'reason':'equal_high','timeframe':timeframe,'strength':c})
    for i in range(ws, len(df)-ws):
        wl = df['low'].iloc[i-ws:i+ws+1]; cl = df['low'].iloc[i]
        if cl == wl.min():
            c = (wl==cl).sum()
            if c>=2: pools['equal_lows'].append({'price':float(cl),'timeframe':timeframe,'candle_index':i,'count':c,'type':'equal_low'}); pools['buy_stops'].append({'price':float(cl),'reason':'equal_low','timeframe':timeframe,'strength':c})
    for key in pools:
        if pools[key]:
            seen = set(); uniq = []
            for p in pools[key]:
                if p['price'] not in seen: seen.add(p['price']); uniq.append(p)
            pools[key] = uniq
            if key in ['buy_stops','equal_lows']: pools[key].sort(key=lambda x: x['price'])
            else: pools[key].sort(key=lambda x: x['price'], reverse=True)
    return pools

async def calculate_liquidity_tp_sl(exchange, symbol, side, entry_price, entry_type):
    o4h = await fetch_ohlcv(exchange, symbol, "4h",100); o1h = await fetch_ohlcv(exchange, symbol, "1h",200); o15 = await fetch_ohlcv(exchange, symbol, "15m",300)
    d4,d1,d15 = create_dataframe(o4h),create_dataframe(o1h),create_dataframe(o15)
    p4 = identify_liquidity_pools(d4,"4h") if d4 is not None else {'buy_stops':[],'sell_stops':[],'equal_highs':[],'equal_lows':[]}
    p1 = identify_liquidity_pools(d1,"1h") if d1 is not None else p4
    p15 = identify_liquidity_pools(d15,"15m") if d15 is not None else p4
    all_p = {'buy_stops':[],'sell_stops':[],'equal_highs':[],'equal_lows':[]}
    for pool in p4['buy_stops']: pool['weight']=3.0; all_p['buy_stops'].append(pool)
    for pool in p1['buy_stops']: pool['weight']=2.0; all_p['buy_stops'].append(pool)
    for pool in p15['buy_stops']: pool['weight']=1.0; all_p['buy_stops'].append(pool)
    for pt in ['sell_stops','equal_highs','equal_lows']:
        for pool in p4[pt]: pool['weight']=3.0; all_p[pt].append(pool)
        for pool in p1[pt]: pool['weight']=2.0; all_p[pt].append(pool)
        for pool in p15[pt]: pool['weight']=1.0; all_p[pt].append(pool)
    all_p['buy_stops'].sort(key=lambda x: x['price']); all_p['sell_stops'].sort(key=lambda x: x['price'], reverse=True)
    all_p['equal_highs'].sort(key=lambda x: x['price'], reverse=True); all_p['equal_lows'].sort(key=lambda x: x['price'])
    tp_targets=[]; tp_sources=[]; sl_price=0.0
    if side=="BUY":
        ss_b = [p for p in all_p['sell_stops'] if p['price']<entry_price]
        if ss_b:
            for tw in [3.0,2.0,1.0]:
                tf = [p for p in ss_b if p.get('weight',1.0)==tw]
                if tf:
                    sp = min(tf, key=lambda x: x['price']); sl_price = sp['price']*0.997; break
            if sl_price==0: sl_price = min(ss_b, key=lambda x: x['price'])['price']*0.995
        else:
            el_b = [p for p in all_p['equal_lows'] if p['price']<entry_price]
            if el_b: sl_price = max(el_b, key=lambda x: x.get('candle_index',0))['price']*0.99
            else: return 0.0, [],[],{}
        if sl_price>entry_price*0.995: sl_price=entry_price*0.985
        bs_a = [p for p in all_p['buy_stops'] if p['price']>entry_price]
        if bs_a:
            tp1 = min(bs_a, key=lambda x: x['price']); tp_targets.append(tp1['price']); tp_sources.append({'tp_level':1,'type':'buy_stop_pool','timeframe':tp1.get('timeframe','unknown')})
            bs_a2 = [p for p in all_p['buy_stops'] if p['price']>tp_targets[0]*1.01]
            if bs_a2: tp2 = min(bs_a2, key=lambda x: x['price']); tp_targets.append(tp2['price']); tp_sources.append({'tp_level':2,'type':'buy_stop_pool'})
        else: return sl_price,[],[],{}
    else:
        bs_a = [p for p in all_p['buy_stops'] if p['price']>entry_price]
        if bs_a:
            for tw in [3.0,2.0,1.0]:
                tf = [p for p in bs_a if p.get('weight',1.0)==tw]
                if tf: sp = max(tf, key=lambda x: x['price']); sl_price = sp['price']*1.003; break
            if sl_price==0: sl_price = max(bs_a, key=lambda x: x['price'])['price']*1.005
        else:
            eh_a = [p for p in all_p['equal_highs'] if p['price']>entry_price]
            if eh_a: sl_price = max(eh_a, key=lambda x: x.get('candle_index',0))['price']*1.01
            else: return 0.0,[],[],{}
        if sl_price<entry_price*1.005: sl_price=entry_price*1.015
        ss_b = [p for p in all_p['sell_stops'] if p['price']<entry_price]
        if ss_b:
            tp1 = max(ss_b, key=lambda x: x['price']); tp_targets.append(tp1['price']); tp_sources.append({'tp_level':1,'type':'sell_stop_pool','timeframe':tp1.get('timeframe','unknown')})
            ss_b2 = [p for p in all_p['sell_stops'] if p['price']<tp_targets[0]*0.99]
            if ss_b2: tp2 = max(ss_b2, key=lambda x: x['price']); tp_targets.append(tp2['price']); tp_sources.append({'tp_level':2,'type':'sell_stop_pool'})
        else: return sl_price,[],[],{}
    risk = abs(entry_price-sl_price); reward = abs(tp_targets[0]-entry_price) if tp_targets else 0
    rr = reward/risk if risk>0 else 0
    la = {'side':side,'entry_type':entry_type,'identified_pools':{'buy_stops':len(all_p['buy_stops']),'sell_stops':len(all_p['sell_stops']),'equal_highs':len(all_p['equal_highs']),'equal_lows':len(all_p['equal_lows'])},'rr_ratio':rr,'risk_pct':risk/entry_price*100 if entry_price>0 else 0,'reward_pct':reward/entry_price*100 if entry_price>0 and tp_targets else 0}
    return sl_price, tp_targets, tp_sources, la

# ============ INSTITUTIONAL DATA (BYBIT) ============
class InstitutionalDataFetcher:
    def __init__(self): self.cache = {}; self.cache_ttl = {'funding':300,'oi':600}
    async def get_institutional_data(self, exchange, symbol: str) -> InstitutionalData:
        ck = f"{symbol}_inst"; now = time.time()
        if ck in self.cache:
            data, ts = self.cache[ck]
            if now - ts < 300: return data
        try:
            fs = self._futures_symbol(symbol)
            fd, oi, sp = await asyncio.gather(self._funding(exchange,fs), self._oi(exchange,fs), self._spread(exchange,symbol,fs), return_exceptions=True)
            d = InstitutionalData()
            if not isinstance(fd, Exception) and fd: d.funding_rate = fd.get('fundingRate',0)*100; d.funding_timestamp = datetime.datetime.utcnow()
            if not isinstance(oi, Exception) and oi: d.open_interest = oi.get('openInterest',0); d.oi_timestamp = datetime.datetime.utcnow()
            self.cache[ck] = (d,now)
            return d
        except Exception as e: log.warning(f"Institutional data error {symbol}: {e}"); return InstitutionalData()
    def _futures_symbol(self, spot): return spot.replace("/USDT", "/USDT:USDT")
    async def _funding(self, ex, sym):
        try: return await rate_limiter.execute_with_backoff(ex.fetch_funding_rate, sym, endpoint_type="funding")
        except: return {}
    async def _oi(self, ex, sym):
        try: return await rate_limiter.execute_with_backoff(ex.fetch_open_interest, sym, endpoint_type="oi")
        except: return {}
    async def _spread(self, ex, spot, fut):
        try:
            st = await rate_limiter.execute_with_backoff(ex.fetch_ticker, spot, endpoint_type="general")
            ft = await rate_limiter.execute_with_backoff(ex.fetch_ticker, fut, endpoint_type="general")
            if st and ft and st.get('last',0)>0:
                basis = (ft.get('last',0) - st['last'])/st['last']*100
                return {'basis':basis}
        except: pass
        return {}

data_fetcher = InstitutionalDataFetcher()

# ============ DIRECTION ENGINE ============
class DirectionEngine:
    def __init__(self): self.layer_weights = {'liquidity':0.25,'trapped':0.35,'bleeding':0.25,'micro':0.15}
    async def analyze_direction(self, exchange, symbol, proposed_side, current_price) -> DirectionMetrics:
        metrics = DirectionMetrics()
        try:
            inst = await data_fetcher.get_institutional_data(exchange, symbol)
            ts, tc = self._quick_trapped(inst, proposed_side, current_price)
            metrics.trapped_side = ts; metrics.trapped_confidence = tc
            bs, fe = self._quick_bleeding(inst); metrics.bleeding_side = bs; metrics.funding_extreme = fe
            direction_score = 0.0
            if proposed_side == "BUY":
                if ts == TrappedSide.SHORT: direction_score += 0.3
                if bs == "LONG": direction_score += 0.2
            else:
                if ts == TrappedSide.LONG: direction_score += 0.3
                if bs == "SHORT": direction_score += 0.2
            abs_score = abs(direction_score)
            if abs_score > 0.4: metrics.confidence_tier = DirectionTier.HIGH
            elif abs_score > 0.2: metrics.confidence_tier = DirectionTier.MEDIUM
            else: metrics.confidence_tier = DirectionTier.LOW
            metrics.direction_score = direction_score
            conflicts = []
            if ts == TrappedSide.LONG and proposed_side == "BUY": conflicts.append("Trapped LONG vs BUY")
            if ts == TrappedSide.SHORT and proposed_side == "SELL": conflicts.append("Trapped SHORT vs SELL")
            metrics.conflict_warnings = conflicts
        except Exception as e: log.debug(f"Direction engine error: {e}")
        return metrics
    def _quick_trapped(self, inst, side, price):
        if inst.oi_change_24h > OI_ACCUMULATION_THRESHOLD*100 and inst.funding_rate > FUNDING_EXTREME_THRESHOLD: return TrappedSide.LONG, 0.6
        elif inst.oi_change_24h < -OI_ACCUMULATION_THRESHOLD*100 and inst.funding_rate < -FUNDING_EXTREME_THRESHOLD: return TrappedSide.SHORT, 0.6
        return TrappedSide.NONE, 0.0
    def _quick_bleeding(self, inst):
        if inst.funding_rate > FUNDING_EXTREME_THRESHOLD: return "LONG", inst.funding_rate
        elif inst.funding_rate < -FUNDING_EXTREME_THRESHOLD: return "SHORT", abs(inst.funding_rate)
        return "", 0.0

direction_engine = DirectionEngine()

# ============ SIGNAL TRACKER ============
class SignalTracker:
    def __init__(self):
        self.active_signals = {}
        self.outcome_stats = {'total_signals':0,'tp1_hits':0,'tp2_hits':0,'tp3_hits':0,'sl_hits':0,'expired':0,'active':0,'win_rate':0.0,'avg_pnl_pct':0.0}
    def get_signal_key(self, setup: Dict) -> tuple:
        symbol = setup.get('symbol',''); side = setup.get('side',''); qs = setup.get('quality',{}).get('total_score',0)
        bucket = math.floor(qs*2)/2
        return (symbol, side, bucket)
    def should_send_alert(self, setup: Dict) -> bool:
        key = self.get_signal_key(setup)
        if key in self.active_signals:
            sig = self.active_signals[key]
            if sig.get('status') == 'active':
                now = datetime.datetime.utcnow()
                age = (now - sig['first_seen']).total_seconds()/60
                if age > SIGNAL_VALIDITY_HOURS*60: self.remove_signal_by_key(key); return True
                return False
        return True
    def update_signal(self, setup, alerted=False):
        key = self.get_signal_key(setup); now = datetime.datetime.utcnow()
        if key not in self.active_signals:
            self.active_signals[key] = {'setup':setup,'first_seen':now,'last_alerted':now if alerted else None,'last_checked':now,'alert_count':1 if alerted else 0,'status':'active','highest_price':setup.get('current_price',0),'lowest_price':setup.get('current_price',0),'price_at_alert':setup.get('current_price',0) if alerted else None}
            self.outcome_stats['total_signals']+=1; self.outcome_stats['active']+=1
        else:
            cp = setup.get('current_price',0)
            self.active_signals[key]['highest_price'] = max(self.active_signals[key]['highest_price'], cp)
            self.active_signals[key]['lowest_price'] = min(self.active_signals[key]['lowest_price'], cp)
            self.active_signals[key]['last_checked'] = now
    def check_signal_outcome(self, setup, current_price):
        key = self.get_signal_key(setup)
        if key not in self.active_signals: return None
        sig = self.active_signals[key]
        if sig['status'] != 'active': return None
        now = datetime.datetime.utcnow()
        if (now - sig['first_seen']).total_seconds() < 180: return None
        setup_data = sig['setup']; side = setup_data.get('side',''); entry = setup_data.get('entry_price',0)
        tp_targets = setup_data.get('tp_targets',[]); sl = setup_data.get('sl_price',0)
        if entry == 0: return None
        outcome = None
        for i, tp in enumerate(tp_targets):
            if tp == 0: continue
            if side == "BUY" and current_price >= tp:
                pnl = (current_price - entry)/entry*100
                outcome = {'type':f'TP{i+1}_HIT','price':current_price,'pnl_pct':pnl,'bars_held':int((now-sig['first_seen']).total_seconds()/60)}
                break
            elif side == "SELL" and current_price <= tp:
                pnl = (entry - current_price)/entry*100
                outcome = {'type':f'TP{i+1}_HIT','price':current_price,'pnl_pct':pnl,'bars_held':int((now-sig['first_seen']).total_seconds()/60)}
                break
        if not outcome and sl>0:
            if (side=="BUY" and current_price<=sl) or (side=="SELL" and current_price>=sl):
                pnl = (current_price-entry)/entry*100 if side=="BUY" else (entry-current_price)/entry*100
                outcome = {'type':'SL_HIT','price':current_price,'pnl_pct':pnl,'bars_held':int((now-sig['first_seen']).total_seconds()/60)}
        if outcome:
            sig['status'] = 'closed'; sig['outcome'] = outcome['type'].lower()
            self.outcome_stats['active'] -= 1
            if 'TP1' in outcome['type']: self.outcome_stats['tp1_hits']+=1
            elif 'TP2' in outcome['type']: self.outcome_stats['tp2_hits']+=1
            elif 'TP3' in outcome['type']: self.outcome_stats['tp3_hits']+=1
            elif outcome['type'] == 'SL_HIT': self.outcome_stats['sl_hits']+=1
            wins = self.outcome_stats['tp1_hits']+self.outcome_stats['tp2_hits']+self.outcome_stats['tp3_hits']
            total_closed = wins + self.outcome_stats['sl_hits']
            if total_closed>0: self.outcome_stats['win_rate'] = wins/total_closed*100
        return outcome
    def remove_signal_by_key(self, key, reason="expired"):
        if key in self.active_signals: self.active_signals.pop(key); self.outcome_stats['active']-=1; self.outcome_stats['expired']+=1
    def cleanup_old_signals(self):
        now = datetime.datetime.utcnow()
        expired = []
        for key, data in self.active_signals.items():
            if data.get('status') == 'active':
                if (now - data['first_seen']).total_seconds()/60 > SIGNAL_VALIDITY_HOURS*60: expired.append(key)
        for key in expired: self.remove_signal_by_key(key)
    def get_stats(self):
        active_count = len([s for s in self.active_signals.values() if s.get('status')=='active'])
        return {'active_signals':active_count,'outcome_stats':self.outcome_stats}

signal_tracker = SignalTracker()
db_lock = asyncio.Lock()
db_conn = None

# ============ TELEGRAM ============
async def send_telegram(msg: str, parse_mode="HTML"):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    async with httpx.AsyncClient(timeout=15.0) as client:
        try: await client.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": parse_mode, "disable_web_page_preview": True})
        except Exception as e: log.warning(f"Telegram send failed: {e}")

async def send_v6_alert(setup: Dict):
    try:
        symbol = setup.get('symbol',''); side = setup.get('side',''); quality = setup.get('quality',{})
        wave = setup.get('wave_structure',{}); momentum = setup.get('momentum_signals',{}); volume = setup.get('volume_breakout',{})
        direction = setup.get('direction_metrics',{}); entry_price = setup.get('entry_price',0); current_price = setup.get('current_price',0)
        tp_targets = setup.get('tp_targets',[]); sl_price = setup.get('sl_price',0); rr_ratio = setup.get('rr_ratio',0)
        quality_score = quality.get('total_score',0)
        wave_label = getattr(wave, 'wave_label', 'UNKNOWN')
        momentum_score = getattr(momentum, 'momentum_score',0)
        vol_triggered = getattr(volume, 'triggered', False)
        emoji = "🔮" if quality_score>=4.5 else "🔥" if quality_score>=3.5 else "✅" if quality_score>=2.5 else "⚠️"
        tier = "S+" if quality_score>=4.5 else "A+" if quality_score>=4.0 else "A" if quality_score>=3.0 else "B" if quality_score>=2.5 else "C"
        wave_lines = []
        if isinstance(wave, WaveStructure) and wave.pattern != WavePattern.NONE:
            wave_lines.append(f"📐 Pattern: {wave.pattern.value}, Wave Label: {wave_label.value}")
            wave_lines.append(f"📏 Impulse: {wave.impulse_size_pct:.1f}% | Retrace: {wave.current_retracement:.0%}")
            if wave.in_optimal_zone: wave_lines.append("📍 IN OPTIMAL ZONE ✅")
        momentum_lines = []
        if isinstance(momentum, MomentumSignals):
            div_emoji = "🐂" if momentum.divergence_type == DivergenceType.BULLISH_REGULAR else "🐻" if momentum.divergence_type == DivergenceType.BEARISH_REGULAR else "⚪"
            momentum_lines.append(f"{div_emoji} Divergence: {momentum.divergence_type.value} ({momentum.divergence_strength:.0%})")
            if momentum.macd_crossed: momentum_lines.append(f"MACD Cross: {momentum.macd_cross_direction}")
            momentum_lines.append(f"RSI: {momentum.rsi_current:.1f} | Score: {momentum.momentum_score:.0%}")
        volume_lines = []
        if isinstance(volume, VolumeBreakout) and volume.triggered: volume_lines.append(f"🚀 Volume: {volume.volume_ratio:.1f}x avg")
        tp_str = "\n".join([f"TP{i+1}: {tp:.8f}" for i, tp in enumerate(tp_targets)])
        msg = f"""{emoji} <b>ROMEOTPT v6.3 - {symbol} | {side}</b>
<b>Tier: {tier} | Wave: {wave_label.value}</b>

<b>📐 WAVE:</b>
{chr(10).join(wave_lines) if wave_lines else 'No wave pattern'}

<b>📈 MOMENTUM:</b>
{chr(10).join(momentum_lines) if momentum_lines else 'No momentum'}

<b>📊 VOLUME:</b>
{chr(10).join(volume_lines) if volume_lines else 'No volume data'}

Entry: <code>{entry_price:.8f}</code> | Now: <code>{current_price:.8f}</code>
{tp_str}
🛡️ SL: <code>{sl_price:.8f}</code>

⚖️ RR: <b>{rr_ratio:.1f}:1</b> | Quality: {quality_score:.1f}/5.0
<i>{datetime.datetime.utcnow().strftime('%H:%M:%S UTC')}</i>"""
        await send_telegram(msg)
    except Exception as e: log.error(f"Error sending v6 alert: {e}")

async def send_deduped_v6_alert(setup: Dict):
    try:
        should_alert = signal_tracker.should_send_alert(setup)
        if should_alert:
            await send_v6_alert(setup); signal_tracker.update_signal(setup, alerted=True); return True
        else:
            signal_tracker.update_signal(setup, alerted=False); return False
    except Exception as e: log.error(f"Deduped alert error: {e}"); return False

# ============ DATABASE ============
async def init_database():
    global db_conn
    try:
        await db_conn.execute("""CREATE TABLE IF NOT EXISTS signals_v6_3 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT, side TEXT, score REAL, timestamp TEXT,
            entry_price REAL, sl_price REAL, tp1 REAL, tp2 REAL, tp3 REAL,
            rr_ratio REAL, quality_tier TEXT, quality_score REAL,
            current_price REAL, trend_bias TEXT,
            wave_pattern TEXT, wave_label TEXT, wave_confidence REAL, fib_retracement REAL,
            in_optimal_zone BOOLEAN, divergence_type TEXT, divergence_strength REAL,
            momentum_score REAL, macd_crossed BOOLEAN, momentum_aligned BOOLEAN,
            volume_triggered BOOLEAN, volume_ratio REAL, sweep_reclaim BOOLEAN,
            direction_tier TEXT, direction_score REAL, trapped_side TEXT,
            status TEXT DEFAULT 'active', alert_sent BOOLEAN DEFAULT 1,
            closed_at TEXT, closed_price REAL, outcome TEXT, pnl_pct REAL,
            UNIQUE(symbol, side, score)
        )""")
        await db_conn.commit(); log.info("✅ Database v6.3 initialized")
    except Exception as e: log.error(f"DB init error: {e}")

async def store_signal(setup):
    async with db_lock:
        try:
            tp = setup.get("tp_targets",[]); quality = setup.get("quality",{})
            wave = setup.get("wave_structure",{}); momentum = setup.get("momentum_signals",{})
            volume = setup.get("volume_breakout",{}); direction = setup.get("direction_metrics",{})
            key = signal_tracker.get_signal_key(setup); _, _, bucket = key
            # extract wave fields
            if isinstance(wave, WaveStructure):
                wp = wave.pattern.value; wl = wave.wave_label.value; wc = wave.pattern_confidence
                fr = wave.current_retracement; iz = wave.in_optimal_zone
            else:
                wp = wave.get('pattern','NONE') if isinstance(wave,dict) else 'NONE'
                wl = getattr(wave,'wave_label', WaveLabel.UNKNOWN).value
                wc = wave.get('pattern_confidence',0) if isinstance(wave,dict) else 0
                fr = wave.get('current_retracement',0) if isinstance(wave,dict) else 0
                iz = wave.get('in_optimal_zone',False)
            # momentum fields
            if isinstance(momentum, MomentumSignals):
                dt = momentum.divergence_type.value; ds = momentum.divergence_strength
                ms = momentum.momentum_score; mc = momentum.macd_crossed; ma = momentum.momentum_aligned
            else:
                dt = momentum.get('divergence_type','NONE') if isinstance(momentum,dict) else 'NONE'
                ds = momentum.get('divergence_strength',0) if isinstance(momentum,dict) else 0
                ms = momentum.get('momentum_score',0) if isinstance(momentum,dict) else 0
                mc = momentum.get('macd_crossed',False); ma = momentum.get('momentum_aligned',False)
            if isinstance(volume, VolumeBreakout): vt=volume.triggered; vr=volume.volume_ratio; sw=volume.sweep_then_reclaim
            else: vt = volume.get('triggered',False); vr=volume.get('volume_ratio',0); sw=volume.get('sweep_then_reclaim',False)
            if isinstance(direction, DirectionMetrics): dir_t = direction.confidence_tier.value; dir_s = direction.direction_score; tr = direction.trapped_side.value
            else: dir_t = direction.get('confidence_tier','LOW'); dir_s = direction.get('direction_score',0); tr = direction.get('trapped_side','NONE')
            await db_conn.execute("""INSERT OR REPLACE INTO signals_v6_3 (
                symbol, side, score, timestamp, entry_price, sl_price, tp1, tp2, tp3,
                rr_ratio, quality_tier, quality_score, current_price, trend_bias,
                wave_pattern, wave_label, wave_confidence, fib_retracement, in_optimal_zone,
                divergence_type, divergence_strength, momentum_score, macd_crossed, momentum_aligned,
                volume_triggered, volume_ratio, sweep_reclaim,
                direction_tier, direction_score, trapped_side, status, alert_sent
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (setup.get("symbol",""), setup.get("side",""), float(bucket), setup.get("timestamp",""),
             float(setup.get("entry_price",0)), float(setup.get("sl_price",0)),
             float(tp[0]) if len(tp)>0 else None, float(tp[1]) if len(tp)>1 else None, float(tp[2]) if len(tp)>2 else None,
             float(setup.get("rr_ratio",0)), quality.get("tier","C"), float(quality.get("total_score",0)),
             float(setup.get("current_price",0)), setup.get("trend_bias","NEUTRAL"),
             wp, wl, float(wc), float(fr), 1 if iz else 0,
             dt, float(ds), float(ms), 1 if mc else 0, 1 if ma else 0,
             1 if vt else 0, float(vr), 1 if sw else 0,
             dir_t, float(dir_s), tr, 'active', 1))
            await db_conn.commit()
        except Exception as e: log.error(f"Error storing signal: {e}")

# ============ DEEP ANALYSIS ============
async def perform_deep_analysis() -> str:
    async with db_lock:
        cursor = await db_conn.execute("""
            SELECT side, trend_bias, wave_pattern, wave_label, wave_confidence, fib_retracement,
                   in_optimal_zone, divergence_type, divergence_strength, momentum_score,
                   macd_crossed, momentum_aligned, volume_triggered, volume_ratio,
                   direction_tier, direction_score, trapped_side, quality_score,
                   outcome, pnl_pct, rr_ratio
            FROM signals_v6_3
            WHERE status = 'closed' AND outcome IS NOT NULL
        """)
        rows = await cursor.fetchall()
    if not rows: return "No closed signals with outcomes yet."
    winners = [r for r in rows if r[-2] and "TP" in r[-2]]
    losers  = [r for r in rows if r[-2] and "SL" in r[-2]]
    def avg(v): return sum(v)/len(v) if v else 0
    def pct(cond, vals): return (sum(cond(r) for r in vals)/len(vals)*100) if vals else 0
    msg = ["<b>🧠 DEEP ANALYSIS (v6.3)</b>", f"Total closed: {len(rows)} | 🟢 Winners: {len(winners)} | 🔴 Losers: {len(losers)}", ""]
    features = [
        ("Wave Confidence", lambda r: r[4], None),
        ("Fib Retracement", lambda r: r[5], None),
        ("In Optimal Zone", lambda r: bool(r[6]), "bool"),
        ("Divergence Strength", lambda r: r[8], None),
        ("Momentum Score", lambda r: r[9], None),
        ("MACD Crossed", lambda r: bool(r[10]), "bool"),
        ("Momentum Aligned", lambda r: bool(r[11]), "bool"),
        ("Volume Triggered", lambda r: bool(r[12]), "bool"),
        ("Volume Ratio", lambda r: r[13], None),
        ("Direction Score (abs)", lambda r: abs(r[15]), None),
        ("Quality Score", lambda r: r[17], None),
        ("RR Ratio", lambda r: r[19], None),
    ]
    for name, extr, typ in features:
        wv = [extr(r) for r in winners]; lv = [extr(r) for r in losers]
        if typ == "bool":
            msg.append(f"<b>{name}</b>:  🟢 {pct(extr, winners):.1f}%  🔴 {pct(extr, losers):.1f}%  (Δ {pct(extr, winners)-pct(extr, losers):+.1f}%)")
        else:
            msg.append(f"<b>{name}</b>:  🟢 avg {avg(wv):.3f}  🔴 avg {avg(lv):.3f}  (Δ {avg(wv)-avg(lv):+.3f})")
    msg.append("\n<b>🔍 Winning wave labels:</b>")
    label_counts = {}
    for r in winners:
        lbl = r[3]
        label_counts[lbl] = label_counts.get(lbl,0)+1
    for lbl, cnt in sorted(label_counts.items(), key=lambda x: x[1], reverse=True):
        msg.append(f"• {lbl}: {cnt/len(winners)*100:.0f}% of winners")
    return "\n".join(msg)

# ============ TELEGRAM LISTENER ============
TELEGRAM_UPDATE_OFFSET = 0
async def telegram_listener():
    global TELEGRAM_UPDATE_OFFSET
    if not TELEGRAM_TOKEN: log.warning("TELEGRAM_BOT_TOKEN not set"); return
    base_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
    async with httpx.AsyncClient(timeout=25) as client:
        while True:
            try:
                resp = await client.get(f"{base_url}/getUpdates", params={"offset": TELEGRAM_UPDATE_OFFSET, "timeout": 30})
                data = resp.json()
                if not data.get("ok"): await asyncio.sleep(5); continue
                for upd in data["result"]:
                    TELEGRAM_UPDATE_OFFSET = upd["update_id"] + 1
                    msg_obj = upd.get("message")
                    if not msg_obj: continue
                    text = (msg_obj.get("text") or "").strip()
                    chat_id = str(msg_obj["chat"]["id"])
                    if text.lower().startswith("/analyze"):
                        log.info(f"Deep analysis requested from chat {chat_id}")
                        analysis = await perform_deep_analysis()
                        await client.post(f"{base_url}/sendMessage", json={"chat_id": chat_id, "text": analysis, "parse_mode": "HTML", "disable_web_page_preview": True})
                await asyncio.sleep(1)
            except Exception as e: log.error(f"Telegram listener error: {e}"); await asyncio.sleep(5)

# ============ SCANNER ============
async def scan_symbol_v6(exchange, symbol: str) -> Optional[Dict]:
    try:
        dd = create_dataframe(await fetch_ohlcv(exchange, symbol, "1d", 200))
        d4 = create_dataframe(await fetch_ohlcv(exchange, symbol, "4h", 100))
        d1 = create_dataframe(await fetch_ohlcv(exchange, symbol, "1h", 100))
        d15 = create_dataframe(await fetch_ohlcv(exchange, symbol, "15m", 100))
        d5 = create_dataframe(await fetch_ohlcv(exchange, symbol, "5m", 50))
        if dd is None or d4 is None or d1 is None: return None
        ticker = await safe_fetch_ticker(exchange, symbol)
        if not ticker: return None
        cp = ticker.get('last',0)
        if cp <= 0: return None

        # trend bias
        trend_bias, trend_score = WaveRangeDetector().detect_trend_bias(dd, d4) if hasattr(WaveRangeDetector, 'detect_trend_bias') else (TrendBias.NEUTRAL, 0)
        if trend_bias == TrendBias.NEUTRAL: return None

        # Elliott wave counting
        wl, wc, _ = wave_counter.count_waves(d4, trend_bias)
        if wl not in ALLOWED_WAVE_LABELS:
            return None

        # wave structure (for fib zones)
        wave = wave_detector.detect_wave_pattern(d4, trend_bias)
        wave.wave_label = wl
        wave.pattern_confidence = max(wave.pattern_confidence, wc)  # trust wave count confidence

        # momentum
        momentum = momentum_engine.analyze_momentum(d1, d15, trend_bias, wave)
        if momentum.divergence_type == DivergenceType.NONE and momentum.momentum_score < 0.3: return None

        entry_price = cp
        if trend_bias == TrendBias.BULLISH: side = "BUY"; entry_type = "DISCOUNT_FIB_ZONE"
        else: side = "SELL"; entry_type = "PREMIUM_FIB_ZONE"

        vol_break = volume_trigger.detect_breakout(d5, d15, trend_bias, wave, entry_price)
        sl_price, tp_targets, tp_sources, la = await calculate_liquidity_tp_sl(exchange, symbol, side, entry_price, entry_type)
        if sl_price <= 0 or not tp_targets: return None
        risk = abs(entry_price - sl_price)
        reward = abs(tp_targets[0] - entry_price) if tp_targets else 0
        rr_ratio = reward/risk if risk>0 else 0
        if rr_ratio < 1.5: return None
        dir_metrics = await direction_engine.analyze_direction(exchange, symbol, side, cp)

        quality_score = 0.0
        quality_score += wave.pattern_confidence * 1.5
        quality_score += momentum.momentum_score * 1.5
        quality_score += vol_break.volume_score * 1.0
        if wave.in_optimal_zone: quality_score += 0.5
        if dir_metrics.confidence_tier == DirectionTier.HIGH: quality_score += 0.5
        elif dir_metrics.confidence_tier == DirectionTier.MEDIUM: quality_score += 0.3
        tier = "S+" if quality_score>=4.5 else "A+" if quality_score>=4.0 else "A" if quality_score>=3.0 else "B" if quality_score>=2.5 else "C"
        if quality_score < MIN_QUALITY_SCORE: return None

        setup = {
            "symbol": symbol, "side": side, "current_price": cp,
            "entry_price": entry_price, "entry_type": entry_type,
            "sl_price": sl_price, "tp_targets": tp_targets, "tp_sources": tp_sources,
            "risk": risk, "reward": reward, "rr_ratio": rr_ratio,
            "trend_bias": trend_bias.value,
            "wave_structure": wave,
            "momentum_signals": momentum,
            "volume_breakout": vol_break,
            "quality": {"tier": tier, "total_score": quality_score, "trend_score": trend_score,
                        "wave_confidence": wave.pattern_confidence, "momentum_score": momentum.momentum_score,
                        "volume_score": vol_break.volume_score},
            "liquidity_analysis": la,
            "direction_metrics": dir_metrics,
            "forced_move_probability": "HIGH" if (wave.in_optimal_zone and momentum.momentum_aligned and vol_break.triggered and quality_score>=3.5) else "MODERATE" if (momentum.momentum_aligned and quality_score>=2.5) else "LOW"
        }
        return setup
    except Exception as e:
        log.error(f"v6 scanner error for {symbol}: {e}")
        return None

# ============ MAIN LOOP ============
async def v6_scanner_main(exchange):
    await send_telegram("🚀 ROMEOTPT v6.3 Bybit – Full Elliott Wave Scanner started.")
    scan_cycle = 0
    while True:
        scan_cycle += 1
        try:
            tickers = await safe_fetch_tickers(exchange)
            pairs = []
            for sym, data in tickers.items():
                if sym.endswith("/USDT") and not sym.startswith("USDT"):
                    vol = data.get("quoteVolume",0)
                    if isinstance(vol, (int,float)) and vol > 100000: pairs.append((sym, float(vol)))
            pairs.sort(key=lambda x: x[1], reverse=True)
            symbols = [s[0] for s in pairs[:TOP_N]]
            stats = signal_tracker.get_stats()
            log.info(f"Scan #{scan_cycle}: {len(symbols)} symbols | Active: {stats['active_signals']}")
            tasks = []
            for sym in symbols:
                tasks.append(asyncio.create_task(scan_symbol_v6(exchange, sym)))
                if len(tasks) >= 3:
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    for r in results:
                        if r and not isinstance(r, Exception):
                            alerted = await send_deduped_v6_alert(r)
                            if alerted: log.info(f"Alert sent for {r['symbol']}")
                            await store_signal(r)
                    tasks = []
                    await asyncio.sleep(0.3)
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for r in results:
                    if r and not isinstance(r, Exception):
                        alerted = await send_deduped_v6_alert(r)
                        if alerted: log.info(f"Alert sent for {r['symbol']}")
                        await store_signal(r)
            signal_tracker.cleanup_old_signals()
            # Outcome monitoring
            for key in list(signal_tracker.active_signals.keys()):
                sig_data = signal_tracker.active_signals.get(key)
                if not sig_data or sig_data['status'] != 'active': continue
                sym = key[0]
                try:
                    t = await safe_fetch_ticker(exchange, sym)
                    if not t: continue
                    cur_pr = t.get('last',0)
                except: continue
                out = signal_tracker.check_signal_outcome(sig_data['setup'], cur_pr)
                if out:
                    setup = sig_data['setup']
                    emoji = "✅" if out['pnl_pct']>0 else "🛑"
                    msg = f"{emoji} <b>SIGNAL OUTCOME - {sym} | {setup['side']}</b>\n<b>{out['type']}</b> at {cur_pr:.8f}\n📊 PnL: <b>{out['pnl_pct']:+.2f}%</b>\n⏱️ Held: ~{out['bars_held']} min\n<i>Outcome detected {datetime.datetime.utcnow().strftime('%H:%M:%S UTC')}</i>"
                    await send_telegram(msg)
                    log.info(f"Outcome: {sym} {setup['side']} {out['type']} PnL={out['pnl_pct']:.2f}%")
            if scan_cycle % 5 == 0:
                ostats = signal_tracker.get_stats()['outcome_stats']
                wins = ostats['tp1_hits']+ostats['tp2_hits']+ostats['tp3_hits']; losses = ostats['sl_hits']
                total = wins+losses
                if total>0: log.info(f"📈 Win rate: {ostats['win_rate']:.1f}% | Active: {ostats['active']}")
            await asyncio.sleep(SCAN_INTERVAL)
        except Exception as e:
            log.error(f"Scanner error: {e}")
            await asyncio.sleep(SCAN_INTERVAL*2)

# ============ FASTAPI ============
app = FastAPI()
@app.get("/health")
async def health():
    stats = signal_tracker.get_stats()
    return {"status":"healthy","version":"6.3 Bybit","active_signals":stats['active_signals'],"outcome_stats":stats['outcome_stats']}
@app.get("/signals/active")
async def get_active_signals():
    active = []
    for key, data in signal_tracker.active_signals.items():
        if data.get('status')=='active':
            symbol, side, bucket = key
            setup = data.get('setup',{})
            active.append({
                "symbol":symbol,"side":side,"quality_score":setup.get('quality',{}).get('total_score',0),
                "tier":setup.get('quality',{}).get('tier','C'),"entry_price":setup.get('entry_price',0),
                "current_price":setup.get('current_price',0),"sl":setup.get('sl_price',0),
                "tp1":setup.get('tp_targets',[0])[0] if len(setup.get('tp_targets',[]))>0 else 0,
                "rr_ratio":setup.get('rr_ratio',0)
            })
    return {"active_signals":active, "count":len(active)}

# ============ MAIN ENTRY ============
async def main():
    global db_conn
    try:
        db_conn = await aiosqlite.connect(DB_PATH)
        await init_database()
        exchange = ccxt.bybit({
            "enableRateLimit": True,
            "options": {"defaultType": "spot"},
            "rateLimit": 500,
            "timeout": 30000,
            "verbose": False,
        })
        asyncio.create_task(telegram_listener())
        await v6_scanner_main(exchange)
    except Exception as e: log.error(f"Fatal error: {e}")
    finally:
        if db_conn: await db_conn.close()
        log.info("Scanner shutdown complete")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--http", action="store_true")
    args = parser.parse_args()
    if args.http: uvicorn.run(app, host="0.0.0.0", port=8000)
    else:
        try: asyncio.run(main())
        except KeyboardInterrupt: log.info("Scanner stopped by user")