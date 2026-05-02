Weird cluade


#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ROMEOTPT SCANNER v7.0 - SIGNAL HARVESTER + EDGE DISCOVERY ENGINE
Based on v6.0 Wave Range + Momentum Breakout
ADDED: Full signal logging (50+ variables), outcome tracking, MAE/MFE,
       CRT context, market context snapshot, edge discovery analytics
"""

import os
import time
import asyncio
import logging
import datetime
import json
import math
import aiosqlite
import httpx
import ccxt.async_support as ccxt
import pandas as pd
import numpy as np
from fastapi import FastAPI
import uvicorn
from typing import Dict, List, Optional, Tuple, Any, Set
from dataclasses import dataclass, field
from enum import Enum
from collections import deque
from scipy import stats as scipy_stats

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

# CRT-specific enums added in v7.0
class TurtleSoupType(str, Enum):
    TBS = "TBS"          # Turtle Body Soup — HIGH PROBABILITY
    TWS = "TWS"          # Turtle Wick Soup — LOWER PROBABILITY
    NONE = "NONE"

class CRTPhase(str, Enum):
    ACCUMULATION = "ACCUMULATION"
    MANIPULATION = "MANIPULATION"
    DISTRIBUTION = "DISTRIBUTION"
    NONE = "NONE"

class SessionType(str, Enum):
    ASIA = "ASIA"
    LONDON = "LONDON"
    NEW_YORK = "NEW_YORK"
    OVERLAP = "OVERLAP"
    OFF = "OFF"

class SignalOutcome(str, Enum):
    TP1_HIT = "TP1_HIT"
    TP2_HIT = "TP2_HIT"
    TP3_HIT = "TP3_HIT"
    SL_HIT = "SL_HIT"
    EXPIRED = "EXPIRED"
    ACTIVE = "ACTIVE"

# ---------------- CONFIG ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
DB_PATH = os.getenv("DB_PATH", "/app/data/romeopt_v7_0.db")

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

# v7.0: Accept ALL signals above this floor — let data decide what works
MIN_QUALITY_SCORE = float(os.getenv("MIN_QUALITY_SCORE", 0.5))

SIGNAL_COOLDOWN_MINUTES = int(os.getenv("SIGNAL_COOLDOWN_MINUTES", 15))
SIGNAL_VALIDITY_HOURS = int(os.getenv("SIGNAL_VALIDITY_HOURS", 48))

MAX_REQUESTS_PER_SECOND = int(os.getenv("MAX_REQUESTS_PER_SECOND", 4))
RATE_LIMIT_RETRIES = int(os.getenv("RATE_LIMIT_RETRIES", 3))
RATE_LIMIT_BACKOFF_FACTOR = float(os.getenv("RATE_LIMIT_BACKOFF_FACTOR", 2.5))

# Edge discovery runs after this many resolved signals
EDGE_DISCOVERY_MIN_SIGNALS = int(os.getenv("EDGE_DISCOVERY_MIN_SIGNALS", 200))

# ---------------- LOGGING ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("romeopt_v7_0")

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

# ============ v7.0 NEW: MARKET CONTEXT SNAPSHOT ============
@dataclass
class MarketContext:
    """
    Full market context captured at signal time.
    Every field logged to DB for edge discovery.
    Nothing filtered out — let data decide what matters.
    """
    # === TIME CONTEXT ===
    hour_utc: int = 0
    minute_utc: int = 0
    day_of_week: int = 0        # 0=Monday
    week_of_month: int = 0
    month: int = 0
    session: SessionType = SessionType.OFF
    is_romeo_key_hour: bool = False   # 1,5,9am EST or 3,6,9am EST
    is_high_prob_day: bool = False    # Tue/Wed/Thu
    is_first_week: bool = False       # First week of month

    # === TREND CONTEXT ===
    trend_bias: str = "NEUTRAL"
    trend_score: float = 0.0
    daily_sma50: float = 0.0
    daily_sma200: float = 0.0
    h4_sma50: float = 0.0
    h4_sma200: float = 0.0
    price_vs_daily_sma50_pct: float = 0.0   # % above/below
    price_vs_daily_sma200_pct: float = 0.0
    daily_sma_golden_cross: bool = False     # sma50 > sma200
    h4_sma_golden_cross: bool = False

    # === WAVE CONTEXT ===
    wave_pattern: str = "NONE"
    wave_confidence: float = 0.0
    impulse_size_pct: float = 0.0
    fib_retracement: float = 0.0
    in_optimal_zone: bool = False
    distance_to_zone_pct: float = 999.0
    inside_bar_count: int = 0        # More = higher probability per Romeo
    fib_500: float = 0.0
    fib_618: float = 0.0
    fib_705: float = 0.0

    # === CRT CONTEXT (v7.0 NEW) ===
    crt_phase: CRTPhase = CRTPhase.NONE
    turtle_soup_type: TurtleSoupType = TurtleSoupType.NONE
    ts_wick_size_pct: float = 0.0    # how far wick extended beyond range
    ts_body_closed_inside: bool = False  # TBS vs TWS classifier
    csd_confirmed: bool = False          # Change in State of Delivery
    c3_entry: bool = False               # Is this a Candle 3 distribution entry
    at_prev_day_hl: bool = False         # At previous day high/low
    at_prev_week_hl: bool = False        # At previous week high/low
    at_session_hl: bool = False          # At session high/low
    asia_range_high: float = 0.0
    asia_range_low: float = 0.0
    london_broke_asia: bool = False      # London broke Asia range = valid TS signal
    ts_within_asia_range: bool = False

    # === MOMENTUM CONTEXT ===
    divergence_type: str = "NONE"
    divergence_strength: float = 0.0
    rsi_current: float = 50.0
    rsi_in_zone: bool = False      # >40 for bull, <60 for bear
    macd_crossed: bool = False
    macd_cross_direction: str = ""
    macd_histogram_reversal: bool = False
    momentum_score: float = 0.0
    momentum_aligned: bool = False

    # === VOLUME CONTEXT ===
    volume_triggered: bool = False
    volume_ratio: float = 0.0       # current / 20-period avg
    sweep_then_reclaim: bool = False
    pattern_break: bool = False
    volume_score: float = 0.0

    # === INSTITUTIONAL CONTEXT ===
    funding_rate: float = 0.0
    funding_extreme: bool = False
    funding_bleeding_side: str = ""
    oi_change_24h: float = 0.0
    trapped_side: str = "NONE"
    trapped_confidence: float = 0.0
    direction_score: float = 0.0
    direction_tier: str = "LOW"

    # === LIQUIDITY CONTEXT ===
    nearest_buy_stop_dist_pct: float = 0.0
    nearest_sell_stop_dist_pct: float = 0.0
    liquidity_pools_count: int = 0
    rr_ratio: float = 0.0
    risk_pct: float = 0.0          # SL distance as % of price

    # === VOLATILITY CONTEXT ===
    atr_14_pct: float = 0.0        # ATR as % of price
    atr_expanding: bool = False    # ATR increasing last 5 candles
    candle_range_vs_atr: float = 0.0  # current candle range / ATR
    spread_est_pct: float = 0.0    # estimated spread as % of price

    # === QUALITY CONTEXT ===
    quality_score: float = 0.0
    quality_tier: str = "C"
    forced_move_probability: str = "LOW"


# ============ v7.0 NEW: SIGNAL RECORD (Full 50+ field log) ============
@dataclass
class SignalRecord:
    """
    Complete signal record stored to DB.
    Outcome fields filled in later by OutcomeTracker.
    """
    # Identity
    id: Optional[int] = None
    symbol: str = ""
    timestamp: str = ""
    side: str = ""              # BUY or SELL
    current_price: float = 0.0
    entry_price: float = 0.0
    entry_type: str = ""
    sl_price: float = 0.0
    tp1: float = 0.0
    tp2: float = 0.0
    tp3: float = 0.0
    sl_pips_pct: float = 0.0
    tp1_pips_pct: float = 0.0
    tp2_pips_pct: float = 0.0
    modeled_rr_tp1: float = 0.0
    modeled_rr_tp2: float = 0.0

    # Full context snapshot (stored as JSON for flexibility)
    context_json: str = "{}"

    # Flattened key context fields for direct SQL querying
    hour_utc: int = 0
    day_of_week: int = 0
    session: str = "OFF"
    is_romeo_key_hour: int = 0
    is_high_prob_day: int = 0
    trend_bias: str = "NEUTRAL"
    wave_pattern: str = "NONE"
    wave_confidence: float = 0.0
    fib_retracement: float = 0.0
    in_optimal_zone: int = 0
    turtle_soup_type: str = "NONE"
    ts_body_closed_inside: int = 0   # 1=TBS, 0=TWS
    csd_confirmed: int = 0
    c3_entry: int = 0
    at_prev_day_hl: int = 0
    at_prev_week_hl: int = 0
    london_broke_asia: int = 0
    divergence_type: str = "NONE"
    divergence_strength: float = 0.0
    rsi_current: float = 50.0
    macd_crossed: int = 0
    momentum_aligned: int = 0
    momentum_score: float = 0.0
    volume_triggered: int = 0
    volume_ratio: float = 0.0
    sweep_then_reclaim: int = 0
    funding_rate: float = 0.0
    funding_extreme: int = 0
    oi_change_24h: float = 0.0
    trapped_side: str = "NONE"
    direction_score: float = 0.0
    direction_tier: str = "LOW"
    atr_14_pct: float = 0.0
    quality_score: float = 0.0
    quality_tier: str = "C"
    rr_ratio: float = 0.0

    # Outcome fields (filled by OutcomeTracker)
    status: str = "active"
    outcome: str = "active"
    outcome_tp1: str = "PENDING"
    outcome_tp2: str = "PENDING"
    outcome_tp3: str = "PENDING"
    actual_rr: float = 0.0
    bars_to_tp1: int = 0
    bars_to_sl: int = 0
    max_adverse_pct: float = 0.0    # MAE — max move against entry
    max_favorable_pct: float = 0.0  # MFE — max move in direction
    closed_at: str = ""
    closed_price: float = 0.0
    pnl_pct: float = 0.0
    alert_sent: int = 0


# ============ RATE LIMITER (preserved from v6) ============
class EnhancedRateLimiter:
    def __init__(self):
        self.max_rps = MAX_REQUESTS_PER_SECOND
        self.max_concurrent = MAX_CONCURRENT
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        self.general_requests = []
        self.funding_requests = []
        self.oi_requests = []
        self.min_delay = 0.25
        self.backoff_factor = RATE_LIMIT_BACKOFF_FACTOR
        self.max_retries = RATE_LIMIT_RETRIES

    async def wait_for_endpoint(self, endpoint_type: str = "general"):
        now = time.time()
        if endpoint_type == "funding":
            request_list = self.funding_requests
            cooldown = 1.5
        elif endpoint_type == "oi":
            request_list = self.oi_requests
            cooldown = 2.0
        else:
            request_list = self.general_requests
            cooldown = 1.0

        request_list[:] = [t for t in request_list if now - t < cooldown]

        if len(request_list) >= 1:
            wait_time = cooldown - (now - request_list[0])
            if wait_time > 0:
                wait_time += np.random.uniform(0.1, 0.3)
                await asyncio.sleep(wait_time)

        request_list.append(now)
        await asyncio.sleep(0.1)

    async def execute_with_backoff(self, func, *args, endpoint_type="general", **kwargs):
        async with self.semaphore:
            for attempt in range(self.max_retries):
                try:
                    await self.wait_for_endpoint(endpoint_type)
                    result = await func(*args, **kwargs)
                    extra_delay = {"funding": 0.15, "oi": 0.2, "general": 0.05}.get(endpoint_type, 0.05)
                    await asyncio.sleep(extra_delay)
                    return result
                except Exception as e:
                    error_str = str(e)
                    if any(phrase in error_str for phrase in ["Too Many Requests", "50011", "429", "rate limit"]):
                        wait_time = self.min_delay * (self.backoff_factor ** attempt)
                        wait_time += np.random.uniform(0.2, 0.5)
                        log.warning(f"Rate limited on {endpoint_type}, attempt {attempt+1}/{self.max_retries}, waiting {wait_time:.2f}s")
                        await asyncio.sleep(wait_time)
                    else:
                        raise e
            raise Exception(f"Failed after {self.max_retries} retries")

rate_limiter = EnhancedRateLimiter()


# ============ UTILS ============
async def fetch_ohlcv(exchange, symbol: str, timeframe: str, limit: int = 100):
    try:
        result = await rate_limiter.execute_with_backoff(
            exchange.fetch_ohlcv, symbol, timeframe=timeframe, limit=limit
        )
        return result
    except Exception as e:
        log.debug(f"Failed to fetch {symbol} {timeframe}: {e}")
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
        log.debug(f"Failed to fetch tickers: {e}")
        return {}


# ============ v7.0 NEW: SESSION & TIME CLASSIFIER ============
class SessionClassifier:
    """
    Classifies current UTC time into Romeo's session model.
    All times referenced to EST (UTC-4 or UTC-5).
    """

    # Romeo's exact key hours in EST → stored as UTC offsets
    # EST = UTC-4 (EDT summer) or UTC-5 (EST winter)
    # We use UTC-4 as default (most of trading year)
    ROMEO_KEY_HOURS_EST = {1, 3, 5, 6, 9, 10, 13}  # 1,3,5,9am + 1pm EST

    @staticmethod
    def get_est_hour(dt_utc: datetime.datetime) -> int:
        """Convert UTC to EST (UTC-4, EDT)"""
        return (dt_utc.hour - 4) % 24

    @staticmethod
    def classify(dt_utc: datetime.datetime) -> SessionType:
        hour_est = SessionClassifier.get_est_hour(dt_utc)
        if 20 <= hour_est or hour_est < 2:
            return SessionType.ASIA
        elif 2 <= hour_est < 8:
            return SessionType.LONDON
        elif 7 <= hour_est < 10:
            return SessionType.OVERLAP
        elif 8 <= hour_est < 17:
            return SessionType.NEW_YORK
        return SessionType.OFF

    @staticmethod
    def is_romeo_key_hour(dt_utc: datetime.datetime) -> bool:
        hour_est = SessionClassifier.get_est_hour(dt_utc)
        minute = dt_utc.minute
        for key_hour in SessionClassifier.ROMEO_KEY_HOURS_EST:
            if hour_est == key_hour and minute <= 15:
                return True
        return False

    @staticmethod
    def is_high_prob_day(dt_utc: datetime.datetime) -> bool:
        """Tuesday=1, Wednesday=2, Thursday=3 are highest probability"""
        return dt_utc.weekday() in {1, 2, 3}

    @staticmethod
    def is_first_week(dt_utc: datetime.datetime) -> bool:
        return dt_utc.day <= 7


# ============ v7.0 NEW: CRT CONTEXT DETECTOR ============
class CRTContextDetector:
    """
    Detects CRT-specific signals from price data.
    TBS vs TWS classification, CSD, Asia range, key levels.
    These variables will be logged and analyzed for edge discovery.
    """

    def detect_turtle_soup_type(self, candle_high: float, candle_low: float,
                                 candle_open: float, candle_close: float,
                                 range_high: float, range_low: float,
                                 direction: str) -> Tuple[TurtleSoupType, bool, float]:
        """
        TBS = body closes back inside range = HIGH PROBABILITY
        TWS = only wick swept, body outside = LOWER PROBABILITY
        Returns: (type, body_closed_inside, wick_extension_pct)
        """
        body_high = max(candle_open, candle_close)
        body_low = min(candle_open, candle_close)

        if direction == "BUY":
            # Bullish: wick swept below range_low
            if candle_low < range_low:
                wick_ext = (range_low - candle_low) / range_low * 100
                body_inside = body_low > range_low  # body stayed above level
                ts_type = TurtleSoupType.TBS if body_inside else TurtleSoupType.TWS
                return ts_type, body_inside, wick_ext
        elif direction == "SELL":
            # Bearish: wick swept above range_high
            if candle_high > range_high:
                wick_ext = (candle_high - range_high) / range_high * 100
                body_inside = body_high < range_high  # body stayed below level
                ts_type = TurtleSoupType.TBS if body_inside else TurtleSoupType.TWS
                return ts_type, body_inside, wick_ext

        return TurtleSoupType.NONE, False, 0.0

    def detect_csd(self, df: pd.DataFrame, direction: str, lookback: int = 5) -> bool:
        """
        CSD = Change in State of Delivery
        Bearish CSD: candle closes BELOW previous candle's low
        Bullish CSD: candle closes ABOVE previous candle's high
        Required for distribution confirmation.
        """
        if df is None or len(df) < lookback:
            return False

        recent = df.tail(lookback)
        for i in range(1, len(recent)):
            prev = recent.iloc[i - 1]
            curr = recent.iloc[i]

            if direction == "BUY":
                if curr['close'] > prev['high']:
                    return True
            elif direction == "SELL":
                if curr['close'] < prev['low']:
                    return True

        return False

    def get_asia_range(self, df_1h: pd.DataFrame,
                       dt_utc: datetime.datetime) -> Tuple[float, float]:
        """Extract Asia session high/low from today's candles"""
        if df_1h is None or len(df_1h) < 6:
            return 0.0, 0.0

        try:
            asia_candles = []
            for _, row in df_1h.iterrows():
                ts = datetime.datetime.utcfromtimestamp(row['timestamp'] / 1000) if row['timestamp'] > 1e10 else datetime.datetime.utcfromtimestamp(row['timestamp'])
                hour_est = SessionClassifier.get_est_hour(ts)
                # Asia: 8pm-2am EST
                if hour_est >= 20 or hour_est < 2:
                    asia_candles.append(row)

            if not asia_candles:
                return 0.0, 0.0

            asia_df = pd.DataFrame(asia_candles)
            return float(asia_df['high'].max()), float(asia_df['low'].min())
        except Exception as e:
            log.debug(f"Asia range detection error: {e}")
            return 0.0, 0.0

    def check_at_key_level(self, price: float, df_daily: pd.DataFrame,
                            df_weekly: Optional[pd.DataFrame] = None,
                            tolerance_pct: float = 0.2) -> Tuple[bool, bool, bool]:
        """
        Check if price is near key levels.
        Returns: (at_prev_day_hl, at_prev_week_hl, at_session_hl)
        """
        at_prev_day = False
        at_prev_week = False
        at_session_hl = False

        if df_daily is not None and len(df_daily) >= 2:
            prev_day = df_daily.iloc[-2]
            tol = price * (tolerance_pct / 100)
            at_prev_day = (abs(price - prev_day['high']) <= tol or
                          abs(price - prev_day['low']) <= tol)

        if df_weekly is not None and len(df_weekly) >= 2:
            prev_week = df_weekly.iloc[-2]
            tol = price * (tolerance_pct / 100)
            at_prev_week = (abs(price - prev_week['high']) <= tol or
                           abs(price - prev_week['low']) <= tol)

        return at_prev_day, at_prev_week, at_session_hl

    def count_inside_bars(self, df: pd.DataFrame, lookback: int = 10) -> int:
        """Count consecutive inside bars (more = higher probability accumulation)"""
        if df is None or len(df) < 3:
            return 0

        count = 0
        recent = df.tail(lookback)
        for i in range(1, len(recent)):
            prev = recent.iloc[i - 1]
            curr = recent.iloc[i]
            if curr['high'] <= prev['high'] and curr['low'] >= prev['low']:
                count += 1
            else:
                count = 0  # Reset — only count consecutive
        return count

    def calculate_atr(self, df: pd.DataFrame, period: int = 14) -> float:
        """ATR as % of current price"""
        if df is None or len(df) < period + 1:
            return 0.0
        try:
            high = df['high'].values
            low = df['low'].values
            close = df['close'].values
            tr = np.maximum(high[1:] - low[1:],
                   np.maximum(abs(high[1:] - close[:-1]),
                              abs(low[1:] - close[:-1])))
            atr = np.mean(tr[-period:])
            current_price = close[-1]
            return (atr / current_price * 100) if current_price > 0 else 0.0
        except:
            return 0.0

    def is_atr_expanding(self, df: pd.DataFrame, period: int = 5) -> bool:
        """Is volatility increasing? (expanding ATR = stronger move likely)"""
        if df is None or len(df) < period + 2:
            return False
        try:
            atrs = []
            for i in range(period):
                idx = -(period - i)
                subset = df.iloc[:idx] if idx < 0 else df
                if len(subset) < 15:
                    continue
                high = subset['high'].values
                low = subset['low'].values
                close = subset['close'].values
                tr = np.maximum(high[1:] - low[1:],
                       np.maximum(abs(high[1:] - close[:-1]),
                                  abs(low[1:] - close[:-1])))
                atrs.append(np.mean(tr[-14:]))
            if len(atrs) < 3:
                return False
            return atrs[-1] > atrs[0]
        except:
            return False


crt_detector = CRTContextDetector()


# ============ WAVE RANGE DETECTOR (preserved from v6) ============
class WaveRangeDetector:
    def __init__(self):
        self.min_wave_size_pct = 2.0
        self.max_correction_pct = 0.80

    def detect_trend_bias(self, df_daily, df_4h) -> Tuple[TrendBias, float, Dict]:
        """Returns bias, score, AND raw SMA values for logging"""
        sma_data = {
            'daily_sma50': 0.0, 'daily_sma200': 0.0,
            'h4_sma50': 0.0, 'h4_sma200': 0.0,
            'daily_golden_cross': False, 'h4_golden_cross': False,
            'price_vs_d_sma50_pct': 0.0, 'price_vs_d_sma200_pct': 0.0
        }

        if df_daily is None or df_4h is None:
            return TrendBias.NEUTRAL, 0.0, sma_data

        try:
            df_daily_copy = df_daily.copy()
            df_daily_copy['sma_50'] = df_daily_copy['close'].rolling(window=50).mean()
            df_daily_copy['sma_200'] = df_daily_copy['close'].rolling(window=200).mean()

            current_price = df_daily_copy['close'].iloc[-1]
            sma_50_daily = df_daily_copy['sma_50'].iloc[-1]
            sma_200_daily = df_daily_copy['sma_200'].iloc[-1]

            df_4h_copy = df_4h.copy()
            df_4h_copy['sma_50'] = df_4h_copy['close'].rolling(window=50).mean()
            df_4h_copy['sma_200'] = df_4h_copy['close'].rolling(window=200).mean()

            sma_50_4h = df_4h_copy['sma_50'].iloc[-1]
            sma_200_4h = df_4h_copy['sma_200'].iloc[-1]
            price_4h = df_4h_copy['close'].iloc[-1]

            sma_data['daily_sma50'] = float(sma_50_daily) if not np.isnan(sma_50_daily) else 0.0
            sma_data['daily_sma200'] = float(sma_200_daily) if not np.isnan(sma_200_daily) else 0.0
            sma_data['h4_sma50'] = float(sma_50_4h) if not np.isnan(sma_50_4h) else 0.0
            sma_data['h4_sma200'] = float(sma_200_4h) if not np.isnan(sma_200_4h) else 0.0
            sma_data['daily_golden_cross'] = bool(sma_50_daily > sma_200_daily)
            sma_data['h4_golden_cross'] = bool(sma_50_4h > sma_200_4h)

            if current_price > 0 and sma_50_daily > 0:
                sma_data['price_vs_d_sma50_pct'] = (current_price - sma_50_daily) / sma_50_daily * 100
            if current_price > 0 and sma_200_daily > 0:
                sma_data['price_vs_d_sma200_pct'] = (current_price - sma_200_daily) / sma_200_daily * 100

            daily_bullish = current_price > sma_50_daily and current_price > sma_200_daily
            h4_bullish = price_4h > sma_50_4h and price_4h > sma_200_4h
            daily_bearish = current_price < sma_50_daily and current_price < sma_200_daily
            h4_bearish = price_4h < sma_50_4h and price_4h < sma_200_4h

            if daily_bullish and h4_bullish:
                return TrendBias.BULLISH, 1.0, sma_data
            elif daily_bearish and h4_bearish:
                return TrendBias.BEARISH, 1.0, sma_data
            elif daily_bullish and price_4h > sma_50_4h:
                return TrendBias.BULLISH, 0.7, sma_data
            elif daily_bearish and price_4h < sma_50_4h:
                return TrendBias.BEARISH, 0.7, sma_data
            elif current_price > sma_50_daily and current_price > sma_200_daily:
                return TrendBias.BULLISH, 0.5, sma_data
            elif current_price < sma_50_daily and current_price < sma_200_daily:
                return TrendBias.BEARISH, 0.5, sma_data

            return TrendBias.NEUTRAL, 0.0, sma_data

        except Exception as e:
            log.debug(f"Trend bias detection error: {e}")
            return TrendBias.NEUTRAL, 0.0, sma_data

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
            log.debug(f"ABC correction detection error: {e}")
        return wave

    def _identify_bullish_correction(self, df_4h, highs, lows, closes) -> WaveStructure:
        wave = WaveStructure()
        try:
            swing_highs = self._find_swing_points(highs, is_high=True, window=3)
            swing_lows = self._find_swing_points(lows, is_high=False, window=3)
            if len(swing_highs) < 1 or len(swing_lows) < 2:
                return wave
            recent_swing_highs = sorted(swing_highs, key=lambda x: x['index'])
            recent_swing_lows = sorted(swing_lows, key=lambda x: x['index'])
            for sh in reversed(recent_swing_highs[-5:]):
                impulse_end = sh['price']
                impulse_end_idx = sh['index']
                prev_lows = [sl for sl in recent_swing_lows if sl['index'] < impulse_end_idx]
                if not prev_lows:
                    continue
                impulse_start = prev_lows[-1]['price']
                impulse_size_pct = abs(impulse_end - impulse_start) / impulse_start * 100
                if impulse_size_pct < self.min_wave_size_pct:
                    continue
                later_lows = [sl for sl in recent_swing_lows if sl['index'] > impulse_end_idx]
                if not later_lows:
                    continue
                correction_end = later_lows[0]['price']
                fib_range = impulse_end - impulse_start
                if fib_range <= 0:
                    continue
                retracement_pct = (impulse_end - correction_end) / fib_range
                if retracement_pct < 0.5 or retracement_pct > 0.8:
                    continue
                wave.pattern = WavePattern.ABC_CORRECTION
                wave.pattern_confidence = min(1.0, impulse_size_pct / 5.0) * min(1.0, retracement_pct)
                wave.impulse_start = impulse_start
                wave.impulse_end = impulse_end
                wave.impulse_size_pct = impulse_size_pct
                wave.correction_start = impulse_end
                wave.correction_end = correction_end
                wave.fib_236 = impulse_end - (fib_range * 0.236)
                wave.fib_382 = impulse_end - (fib_range * 0.382)
                wave.fib_500 = impulse_end - (fib_range * 0.5)
                wave.fib_618 = impulse_end - (fib_range * 0.618)
                wave.fib_705 = impulse_end - (fib_range * 0.705)
                wave.fib_786 = impulse_end - (fib_range * 0.786)
                wave.current_retracement = retracement_pct
                current_close = closes[-1]
                wave.zone_price_high = wave.fib_500
                wave.zone_price_low = wave.fib_705
                wave.in_optimal_zone = wave.fib_705 <= current_close <= wave.fib_500
                if current_close > wave.fib_500:
                    wave.distance_to_zone_pct = (current_close - wave.fib_500) / wave.fib_500 * 100
                elif current_close < wave.fib_705:
                    wave.distance_to_zone_pct = (wave.fib_705 - current_close) / wave.fib_705 * 100
                else:
                    wave.distance_to_zone_pct = 0.0
                wave.candle_count = len(df_4h)
                break
        except Exception as e:
            log.debug(f"Bullish correction error: {e}")
        return wave

    def _identify_bearish_correction(self, df_4h, highs, lows, closes) -> WaveStructure:
        wave = WaveStructure()
        try:
            swing_highs = self._find_swing_points(highs, is_high=True, window=3)
            swing_lows = self._find_swing_points(lows, is_high=False, window=3)
            if len(swing_highs) < 2 or len(swing_lows) < 1:
                return wave
            recent_swing_highs = sorted(swing_highs, key=lambda x: x['index'])
            recent_swing_lows = sorted(swing_lows, key=lambda x: x['index'])
            for sl in reversed(recent_swing_lows[-5:]):
                impulse_end = sl['price']
                impulse_end_idx = sl['index']
                prev_highs = [sh for sh in recent_swing_highs if sh['index'] < impulse_end_idx]
                if not prev_highs:
                    continue
                impulse_start = prev_highs[-1]['price']
                impulse_size_pct = abs(impulse_start - impulse_end) / impulse_start * 100
                if impulse_size_pct < self.min_wave_size_pct:
                    continue
                later_highs = [sh for sh in recent_swing_highs if sh['index'] > impulse_end_idx]
                if not later_highs:
                    continue
                correction_end = later_highs[0]['price']
                fib_range = impulse_start - impulse_end
                if fib_range <= 0:
                    continue
                retracement_pct = (correction_end - impulse_end) / fib_range
                if retracement_pct < 0.5 or retracement_pct > 0.8:
                    continue
                wave.pattern = WavePattern.ABC_CORRECTION
                wave.pattern_confidence = min(1.0, impulse_size_pct / 5.0) * min(1.0, retracement_pct)
                wave.impulse_start = impulse_start
                wave.impulse_end = impulse_end
                wave.impulse_size_pct = impulse_size_pct
                wave.correction_start = impulse_end
                wave.correction_end = correction_end
                wave.fib_236 = impulse_end + (fib_range * 0.236)
                wave.fib_382 = impulse_end + (fib_range * 0.382)
                wave.fib_500 = impulse_end + (fib_range * 0.5)
                wave.fib_618 = impulse_end + (fib_range * 0.618)
                wave.fib_705 = impulse_end + (fib_range * 0.705)
                wave.fib_786 = impulse_end + (fib_range * 0.786)
                wave.current_retracement = retracement_pct
                current_close = closes[-1]
                wave.zone_price_high = wave.fib_705
                wave.zone_price_low = wave.fib_500
                wave.in_optimal_zone = wave.fib_500 <= current_close <= wave.fib_705
                if current_close < wave.fib_500:
                    wave.distance_to_zone_pct = (wave.fib_500 - current_close) / wave.fib_500 * 100
                elif current_close > wave.fib_705:
                    wave.distance_to_zone_pct = (current_close - wave.fib_705) / wave.fib_705 * 100
                else:
                    wave.distance_to_zone_pct = 0.0
                wave.candle_count = len(df_4h)
                break
        except Exception as e:
            log.debug(f"Bearish correction error: {e}")
        return wave

    def _find_swing_points(self, prices, is_high: bool, window: int = 3) -> List[Dict]:
        swing_points = []
        for i in range(window, len(prices) - window):
            if is_high:
                if prices[i] == max(prices[i-window:i+window+1]):
                    left_cond = all(prices[i] > prices[j] for j in range(i-window, i))
                    right_cond = all(prices[i] > prices[j] for j in range(i+1, i+window+1))
                    if left_cond or right_cond:
                        swing_points.append({'index': i, 'price': prices[i]})
            else:
                if prices[i] == min(prices[i-window:i+window+1]):
                    left_cond = all(prices[i] < prices[j] for j in range(i-window, i))
                    right_cond = all(prices[i] < prices[j] for j in range(i+1, i+window+1))
                    if left_cond or right_cond:
                        swing_points.append({'index': i, 'price': prices[i]})
        return swing_points

wave_detector = WaveRangeDetector()


# ============ MOMENTUM ENGINE (preserved from v6) ============
class MomentumDivergenceEngine:
    def __init__(self):
        self.rsi_period = 14
        self.macd_fast = 12
        self.macd_slow = 26
        self.macd_signal = 9

    def analyze_momentum(self, df_1h, df_15m, trend_bias: TrendBias,
                         wave: WaveStructure) -> MomentumSignals:
        momentum = MomentumSignals()
        if df_1h is None or df_15m is None:
            return momentum
        try:
            rsi_1h = self._calculate_rsi(df_1h['close'])
            rsi_15m = self._calculate_rsi(df_15m['close'])
            macd_15m = self._calculate_macd(df_15m['close'])
            momentum.rsi_current = rsi_15m[-1]
            if trend_bias == TrendBias.BULLISH:
                divergence = self._detect_bullish_divergence(df_15m, rsi_15m, df_1h, rsi_1h)
            elif trend_bias == TrendBias.BEARISH:
                divergence = self._detect_bearish_divergence(df_15m, rsi_15m, df_1h, rsi_1h)
            else:
                divergence = DivergenceType.NONE, 0.0, []
            momentum.divergence_type = divergence[0]
            momentum.divergence_strength = divergence[1]
            momentum.divergence_points = divergence[2]
            if len(macd_15m['macd_line']) >= 2 and len(macd_15m['signal_line']) >= 2:
                prev_macd = macd_15m['macd_line'][-2]
                prev_signal = macd_15m['signal_line'][-2]
                curr_macd = macd_15m['macd_line'][-1]
                curr_signal = macd_15m['signal_line'][-1]
                momentum.macd_line = curr_macd
                momentum.macd_signal_line = curr_signal
                momentum.macd_histogram = macd_15m['histogram'][-1]
                if prev_macd < prev_signal and curr_macd > curr_signal:
                    momentum.macd_crossed = True
                    momentum.macd_cross_direction = "BULLISH"
                elif prev_macd > prev_signal and curr_macd < curr_signal:
                    momentum.macd_crossed = True
                    momentum.macd_cross_direction = "BEARISH"
                if len(macd_15m['histogram']) >= 3:
                    hist_3 = macd_15m['histogram'][-3]
                    hist_2 = macd_15m['histogram'][-2]
                    hist_1 = macd_15m['histogram'][-1]
                    if trend_bias == TrendBias.BULLISH and hist_3 < hist_2 < hist_1 and hist_3 < 0:
                        momentum.macd_histogram_reversal = True
                    elif trend_bias == TrendBias.BEARISH and hist_3 > hist_2 > hist_1 and hist_3 > 0:
                        momentum.macd_histogram_reversal = True
            momentum.momentum_score = self._calculate_momentum_score(momentum, trend_bias)
            if trend_bias == TrendBias.BULLISH:
                momentum.momentum_aligned = (
                    momentum.divergence_type == DivergenceType.BULLISH_REGULAR and
                    (momentum.macd_crossed and momentum.macd_cross_direction == "BULLISH" or
                     momentum.macd_histogram_reversal)
                )
            elif trend_bias == TrendBias.BEARISH:
                momentum.momentum_aligned = (
                    momentum.divergence_type == DivergenceType.BEARISH_REGULAR and
                    (momentum.macd_crossed and momentum.macd_cross_direction == "BEARISH" or
                     momentum.macd_histogram_reversal)
                )
        except Exception as e:
            log.debug(f"Momentum analysis error: {e}")
        return momentum

    def _calculate_rsi(self, prices: pd.Series, period: int = 14) -> np.ndarray:
        delta = prices.diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        avg_gain = gain.rolling(window=period).mean()
        avg_loss = loss.rolling(window=period).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))
        return rsi.fillna(50).values

    def _calculate_macd(self, prices: pd.Series) -> Dict:
        ema_fast = prices.ewm(span=self.macd_fast).mean()
        ema_slow = prices.ewm(span=self.macd_slow).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=self.macd_signal).mean()
        histogram = macd_line - signal_line
        return {'macd_line': macd_line.values, 'signal_line': signal_line.values, 'histogram': histogram.values}

    def _detect_bullish_divergence(self, df_15m, rsi_15m, df_1h, rsi_1h):
        try:
            prices = df_15m['low'].values
            lookback = min(50, len(prices) - 5)
            recent_prices = prices[-lookback:]
            recent_rsi = rsi_15m[-lookback:]
            price_lows = self._find_local_lows(recent_prices, window=5)
            if len(price_lows) >= 2:
                last_low = price_lows[-1]
                prev_low = price_lows[-2]
                if last_low['price'] < prev_low['price']:
                    if recent_rsi[last_low['index']] > recent_rsi[prev_low['index']]:
                        strength = self._calculate_divergence_strength(
                            prev_low['price'], last_low['price'],
                            recent_rsi[prev_low['index']], recent_rsi[last_low['index']]
                        )
                        return DivergenceType.BULLISH_REGULAR, strength, []
            if len(rsi_1h) >= 3 and rsi_1h[-1] > 40:
                return DivergenceType.BULLISH_REGULAR, 0.4, []
        except Exception as e:
            log.debug(f"Bullish divergence error: {e}")
        return DivergenceType.NONE, 0.0, []

    def _detect_bearish_divergence(self, df_15m, rsi_15m, df_1h, rsi_1h):
        try:
            prices = df_15m['high'].values
            lookback = min(50, len(prices) - 5)
            recent_prices = prices[-lookback:]
            recent_rsi = rsi_15m[-lookback:]
            price_highs = self._find_local_highs(recent_prices, window=5)
            if len(price_highs) >= 2:
                last_high = price_highs[-1]
                prev_high = price_highs[-2]
                if last_high['price'] > prev_high['price']:
                    if recent_rsi[last_high['index']] < recent_rsi[prev_high['index']]:
                        strength = self._calculate_divergence_strength(
                            prev_high['price'], last_high['price'],
                            recent_rsi[prev_high['index']], recent_rsi[last_high['index']]
                        )
                        return DivergenceType.BEARISH_REGULAR, strength, []
            if len(rsi_1h) >= 3 and rsi_1h[-1] < 60:
                return DivergenceType.BEARISH_REGULAR, 0.4, []
        except Exception as e:
            log.debug(f"Bearish divergence error: {e}")
        return DivergenceType.NONE, 0.0, []

    def _find_local_lows(self, prices, window=3):
        return [{'index': i, 'price': prices[i]}
                for i in range(window, len(prices) - window)
                if prices[i] == min(prices[i-window:i+window+1])]

    def _find_local_highs(self, prices, window=3):
        return [{'index': i, 'price': prices[i]}
                for i in range(window, len(prices) - window)
                if prices[i] == max(prices[i-window:i+window+1])]

    def _calculate_divergence_strength(self, price1, price2, rsi1, rsi2):
        if abs(price1 - price2) < 0.0001:
            return 0.0
        price_change_pct = abs(price2 - price1) / price1 * 100
        rsi_change = abs(rsi2 - rsi1)
        return max(0.0, min(1.0, (price_change_pct / 2.0) * 0.5 + (rsi_change / 10.0) * 0.5))

    def _calculate_momentum_score(self, momentum: MomentumSignals, trend_bias: TrendBias) -> float:
        score = 0.0
        if momentum.divergence_type != DivergenceType.NONE:
            score += 0.4 * momentum.divergence_strength
        if momentum.macd_crossed:
            if ((trend_bias == TrendBias.BULLISH and momentum.macd_cross_direction == "BULLISH") or
                (trend_bias == TrendBias.BEARISH and momentum.macd_cross_direction == "BEARISH")):
                score += 0.3
            else:
                score += 0.1
        if momentum.macd_histogram_reversal:
            score += 0.2
        if trend_bias == TrendBias.BULLISH and momentum.rsi_current > 40:
            score += 0.1
        elif trend_bias == TrendBias.BEARISH and momentum.rsi_current < 60:
            score += 0.1
        return min(1.0, score)

momentum_engine = MomentumDivergenceEngine()


# ============ VOLUME BREAKOUT TRIGGER (preserved from v6) ============
class VolumeBreakoutTrigger:
    def __init__(self):
        self.min_volume_ratio = VOLUME_SPIKE_MULTIPLIER
        self.volume_lookback = 20

    def detect_breakout(self, df_5m, df_15m, trend_bias: TrendBias,
                        wave: WaveStructure, entry_price: float) -> VolumeBreakout:
        breakout = VolumeBreakout()
        if df_5m is None or len(df_5m) < self.volume_lookback + 3:
            return breakout
        if df_15m is None or len(df_15m) < 5:
            return breakout
        try:
            recent_volume = df_5m['volume'].values[-(self.volume_lookback+3):]
            latest_candles = recent_volume[-3:]
            avg_volume_base = recent_volume[:self.volume_lookback]
            if len(avg_volume_base) == 0:
                return breakout
            avg_volume = np.mean(avg_volume_base)
            breakout.avg_volume_20 = avg_volume
            if avg_volume <= 0:
                return breakout
            for i in range(3):
                candle_volume = latest_candles[i]
                volume_ratio = candle_volume / avg_volume
                if volume_ratio < self.min_volume_ratio:
                    continue
                try:
                    candle = df_5m.iloc[-(3-i)]
                except IndexError:
                    continue
                if trend_bias == TrendBias.BULLISH:
                    if wave.in_optimal_zone or wave.distance_to_zone_pct < 2.0:
                        if candle['close'] > wave.fib_500 or candle['high'] > wave.fib_500:
                            breakout.triggered = True
                            breakout.breakout_direction = "BULLISH"
                            breakout.breakout_price = candle['close']
                            breakout.pattern_break = True
                            breakout.volume_ratio = volume_ratio
                            breakout.breakout_candle_volume = candle_volume
                            breakout.volume_score = min(1.0, (volume_ratio - 1.5) / 3.5)
                            if candle['low'] < wave.fib_705 * 0.998:
                                breakout.sweep_then_reclaim = True
                                breakout.volume_score = min(1.0, breakout.volume_score + 0.2)
                            return breakout
                elif trend_bias == TrendBias.BEARISH:
                    if wave.in_optimal_zone or wave.distance_to_zone_pct < 2.0:
                        if candle['close'] < wave.fib_500 or candle['low'] < wave.fib_500:
                            breakout.triggered = True
                            breakout.breakout_direction = "BEARISH"
                            breakout.breakout_price = candle['close']
                            breakout.pattern_break = True
                            breakout.volume_ratio = volume_ratio
                            breakout.breakout_candle_volume = candle_volume
                            breakout.volume_score = min(1.0, (volume_ratio - 1.5) / 3.5)
                            if candle['high'] > wave.fib_705 * 1.002:
                                breakout.sweep_then_reclaim = True
                                breakout.volume_score = min(1.0, breakout.volume_score + 0.2)
                            return breakout
            # High volume decisive candle fallback
            for i in range(3):
                candle_volume = latest_candles[i]
                volume_ratio = candle_volume / avg_volume
                if volume_ratio >= self.min_volume_ratio * 1.2:
                    try:
                        candle = df_5m.iloc[-(3-i)]
                    except IndexError:
                        continue
                    body_size = abs(candle['close'] - candle['open'])
                    total_range = candle['high'] - candle['low']
                    if total_range > 0 and body_size / total_range > 0.6:
                        if trend_bias == TrendBias.BULLISH and candle['close'] > candle['open']:
                            breakout.triggered = True
                            breakout.breakout_direction = "BULLISH"
                            breakout.breakout_price = candle['close']
                            breakout.volume_ratio = volume_ratio
                            breakout.breakout_candle_volume = candle_volume
                            breakout.volume_score = 0.5
                            return breakout
                        elif trend_bias == TrendBias.BEARISH and candle['close'] < candle['open']:
                            breakout.triggered = True
                            breakout.breakout_direction = "BEARISH"
                            breakout.breakout_price = candle['close']
                            breakout.volume_ratio = volume_ratio
                            breakout.breakout_candle_volume = candle_volume
                            breakout.volume_score = 0.5
                            return breakout
        except Exception as e:
            log.debug(f"Volume breakout error: {e}")
        return breakout

volume_trigger = VolumeBreakoutTrigger()


# ============ LIQUIDITY POOLS (preserved from v6) ============
def identify_liquidity_pools(df, timeframe="1h"):
    pools = {'buy_stops': [], 'sell_stops': [], 'equal_highs': [], 'equal_lows': []}
    if df is None or len(df) < 20:
        return pools
    window_size = 5 if timeframe == "15m" else 3
    for i in range(window_size, len(df)-window_size):
        window_highs = df['high'].iloc[i-window_size:i+window_size+1]
        current_high = df['high'].iloc[i]
        if current_high == window_highs.max():
            same_high_count = (window_highs == current_high).sum()
            if same_high_count >= 2:
                pools['equal_highs'].append({'price': float(current_high), 'timeframe': timeframe, 'candle_index': i, 'count': same_high_count, 'type': 'equal_high'})
                pools['sell_stops'].append({'price': float(current_high), 'reason': 'equal_high', 'timeframe': timeframe, 'strength': same_high_count})
    for i in range(window_size, len(df)-window_size):
        window_lows = df['low'].iloc[i-window_size:i+window_size+1]
        current_low = df['low'].iloc[i]
        if current_low == window_lows.min():
            same_low_count = (window_lows == current_low).sum()
            if same_low_count >= 2:
                pools['equal_lows'].append({'price': float(current_low), 'timeframe': timeframe, 'candle_index': i, 'count': same_low_count, 'type': 'equal_low'})
                pools['buy_stops'].append({'price': float(current_low), 'reason': 'equal_low', 'timeframe': timeframe, 'strength': same_low_count})
    for key in pools:
        if pools[key]:
            seen = set()
            pools[key] = [p for p in pools[key] if not (p['price'] in seen or seen.add(p['price']))]
            pools[key].sort(key=lambda x: x['price'], reverse=(key in ['sell_stops', 'equal_highs']))
    return pools


async def calculate_liquidity_tp_sl(exchange, symbol: str, side: str, entry_price: float,
                                    entry_type: str) -> Tuple[float, List[float], List[Dict], Dict]:
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
    for pool in pools_4h['buy_stops']:
        pool['weight'] = 3.0; all_pools['buy_stops'].append(pool)
    for pool in pools_1h['buy_stops']:
        pool['weight'] = 2.0; all_pools['buy_stops'].append(pool)
    for pool in pools_15m['buy_stops']:
        pool['weight'] = 1.0; all_pools['buy_stops'].append(pool)
    for pool_type in ['sell_stops', 'equal_highs', 'equal_lows']:
        for pool in pools_4h[pool_type]:
            pool['weight'] = 3.0; all_pools[pool_type].append(pool)
        for pool in pools_1h[pool_type]:
            pool['weight'] = 2.0; all_pools[pool_type].append(pool)
        for pool in pools_15m[pool_type]:
            pool['weight'] = 1.0; all_pools[pool_type].append(pool)
    all_pools['buy_stops'].sort(key=lambda x: x['price'])
    all_pools['sell_stops'].sort(key=lambda x: x['price'], reverse=True)
    all_pools['equal_highs'].sort(key=lambda x: x['price'], reverse=True)
    all_pools['equal_lows'].sort(key=lambda x: x['price'])
    current_price = entry_price
    tp_targets = []
    tp_sources = []
    sl_price = 0.0
    sl_source = {}
    if side == "BUY":
        sell_stops_below = [p for p in all_pools['sell_stops'] if p['price'] < current_price]
        if sell_stops_below:
            for tw in [3.0, 2.0, 1.0]:
                tpools = [p for p in sell_stops_below if p.get('weight', 1.0) == tw]
                if tpools:
                    sp = min(tpools, key=lambda x: x['price'])
                    sl_price = sp['price'] * 0.997
                    sl_source = {'type': 'sell_stop_pool', 'timeframe': sp.get('timeframe', 'unknown'), 'original_price': sp['price']}
                    break
            if sl_price == 0:
                sl_price = min(sell_stops_below, key=lambda x: x['price'])['price'] * 0.995
        else:
            equal_lows_below = [p for p in all_pools['equal_lows'] if p['price'] < current_price]
            if equal_lows_below:
                sl_price = max(equal_lows_below, key=lambda x: x.get('candle_index', 0))['price'] * 0.99
            else:
                return 0.0, [], [], {}
        if sl_price > current_price * 0.995:
            sl_price = current_price * 0.985
        buy_stops_above = [p for p in all_pools['buy_stops'] if p['price'] > current_price]
        if buy_stops_above:
            tp1_pool = min(buy_stops_above, key=lambda x: x['price'])
            tp_targets.append(tp1_pool['price'])
            tp_sources.append({'tp_level': 1, 'type': 'buy_stop_pool', 'timeframe': tp1_pool.get('timeframe', 'unknown')})
            above_tp1 = [p for p in all_pools['buy_stops'] if p['price'] > tp_targets[0] * 1.01]
            if above_tp1:
                tp2_pool = min(above_tp1, key=lambda x: x['price'])
                tp_targets.append(tp2_pool['price'])
                tp_sources.append({'tp_level': 2, 'type': 'buy_stop_pool', 'timeframe': tp2_pool.get('timeframe', 'unknown')})
        else:
            return sl_price, [], [], {}
    else:
        buy_stops_above = [p for p in all_pools['buy_stops'] if p['price'] > current_price]
        if buy_stops_above:
            for tw in [3.0, 2.0, 1.0]:
                tpools = [p for p in buy_stops_above if p.get('weight', 1.0) == tw]
                if tpools:
                    sp = max(tpools, key=lambda x: x['price'])
                    sl_price = sp['price'] * 1.003
                    sl_source = {'type': 'buy_stop_pool', 'timeframe': sp.get('timeframe', 'unknown')}
                    break
            if sl_price == 0:
                sl_price = max(buy_stops_above, key=lambda x: x['price'])['price'] * 1.005
        else:
            equal_highs_above = [p for p in all_pools['equal_highs'] if p['price'] > current_price]
            if equal_highs_above:
                sl_price = max(equal_highs_above, key=lambda x: x.get('candle_index', 0))['price'] * 1.01
            else:
                return 0.0, [], [], {}
        if sl_price < current_price * 1.005:
            sl_price = current_price * 1.015
        sell_stops_below = [p for p in all_pools['sell_stops'] if p['price'] < current_price]
        if sell_stops_below:
            tp1_pool = max(sell_stops_below, key=lambda x: x['price'])
            tp_targets.append(tp1_pool['price'])
            tp_sources.append({'tp_level': 1, 'type': 'sell_stop_pool', 'timeframe': tp1_pool.get('timeframe', 'unknown')})
            below_tp1 = [p for p in all_pools['sell_stops'] if p['price'] < tp_targets[0] * 0.99]
            if below_tp1:
                tp2_pool = max(below_tp1, key=lambda x: x['price'])
                tp_targets.append(tp2_pool['price'])
                tp_sources.append({'tp_level': 2, 'type': 'sell_stop_pool', 'timeframe': tp2_pool.get('timeframe', 'unknown')})
        else:
            return sl_price, [], [], {}
    risk = abs(current_price - sl_price)
    reward = abs(tp_targets[0] - current_price) if tp_targets else 0
    rr_ratio = reward / risk if risk > 0 else 0
    liquidity_analysis = {
        'side': side, 'entry_type': entry_type,
        'identified_pools': {k: len(v) for k, v in all_pools.items()},
        'sl_source': sl_source, 'tp_sources': tp_sources, 'rr_ratio': rr_ratio,
        'risk_pct': risk / current_price * 100 if current_price > 0 else 0,
        'reward_pct': reward / current_price * 100 if current_price > 0 and tp_targets else 0
    }
    return sl_price, tp_targets, tp_sources, liquidity_analysis


# ============ INSTITUTIONAL DATA FETCHER (preserved from v6) ============
class InstitutionalDataFetcher:
    def __init__(self):
        self.cache = {}

    async def get_institutional_data(self, exchange, symbol: str) -> InstitutionalData:
        cache_key = f"{symbol}_institutional"
        now = time.time()
        if cache_key in self.cache:
            data, timestamp = self.cache[cache_key]
            if now - timestamp < 300:
                return data
        try:
            futures_symbol = symbol.replace("/USDT", "-USDT-SWAP")
            tasks = [
                self._fetch_funding_data(exchange, futures_symbol),
                self._fetch_open_interest(exchange, futures_symbol),
            ]
            funding_data, oi_data = await asyncio.gather(*tasks, return_exceptions=True)
            data = InstitutionalData()
            if not isinstance(funding_data, Exception) and funding_data:
                data.funding_rate = funding_data.get('fundingRate', 0) * 100
                data.funding_timestamp = datetime.datetime.utcnow()
            if not isinstance(oi_data, Exception) and oi_data:
                data.open_interest = oi_data.get('openInterest', 0)
                try:
                    oi_history = await rate_limiter.execute_with_backoff(
                        exchange.fetch_open_interest_history, futures_symbol, '1h', limit=24, endpoint_type="oi"
                    )
                    if oi_history and len(oi_history) >= 2:
                        latest = oi_history[0]['openInterest']
                        oldest = oi_history[-1]['openInterest']
                        if oldest > 0:
                            data.oi_change_24h = (latest - oldest) / oldest * 100
                except:
                    pass
            self.cache[cache_key] = (data, now)
            return data
        except Exception as e:
            log.warning(f"Failed to fetch institutional data for {symbol}: {e}")
            return InstitutionalData()

    async def _fetch_funding_data(self, exchange, futures_symbol: str) -> Dict:
        try:
            return await rate_limiter.execute_with_backoff(exchange.fetch_funding_rate, futures_symbol, endpoint_type="funding")
        except:
            return {}

    async def _fetch_open_interest(self, exchange, futures_symbol: str) -> Dict:
        try:
            return await rate_limiter.execute_with_backoff(exchange.fetch_open_interest, futures_symbol, endpoint_type="oi")
        except:
            return {}

data_fetcher = InstitutionalDataFetcher()


# ============ DIRECTION ENGINE (preserved from v6) ============
class DirectionEngine:
    async def analyze_direction(self, exchange, symbol: str, proposed_side: str,
                                current_price: float) -> DirectionMetrics:
        metrics = DirectionMetrics()
        try:
            institutional_data = await data_fetcher.get_institutional_data(exchange, symbol)
            if institutional_data.oi_change_24h > OI_ACCUMULATION_THRESHOLD * 100 and institutional_data.funding_rate > FUNDING_EXTREME_THRESHOLD:
                metrics.trapped_side = TrappedSide.LONG
                metrics.trapped_confidence = 0.6
            elif institutional_data.oi_change_24h < -OI_ACCUMULATION_THRESHOLD * 100 and institutional_data.funding_rate < -FUNDING_EXTREME_THRESHOLD:
                metrics.trapped_side = TrappedSide.SHORT
                metrics.trapped_confidence = 0.6
            if institutional_data.funding_rate > FUNDING_EXTREME_THRESHOLD:
                metrics.bleeding_side = "LONG"
                metrics.funding_extreme = institutional_data.funding_rate
            elif institutional_data.funding_rate < -FUNDING_EXTREME_THRESHOLD:
                metrics.bleeding_side = "SHORT"
                metrics.funding_extreme = abs(institutional_data.funding_rate)
            direction_score = 0.0
            if proposed_side == "BUY":
                if metrics.trapped_side == TrappedSide.SHORT:
                    direction_score += 0.3
                if metrics.bleeding_side == "LONG":
                    direction_score += 0.2
            else:
                if metrics.trapped_side == TrappedSide.LONG:
                    direction_score += 0.3
                if metrics.bleeding_side == "SHORT":
                    direction_score += 0.2
            metrics.direction_score = direction_score
            abs_score = abs(direction_score)
            metrics.confidence_tier = DirectionTier.HIGH if abs_score > 0.4 else DirectionTier.MEDIUM if abs_score > 0.2 else DirectionTier.LOW
            conflicts = []
            if metrics.trapped_side == TrappedSide.LONG and proposed_side == "BUY":
                conflicts.append("Trapped LONG vs BUY")
            if metrics.trapped_side == TrappedSide.SHORT and proposed_side == "SELL":
                conflicts.append("Trapped SHORT vs SELL")
            metrics.conflict_warnings = conflicts
        except Exception as e:
            log.debug(f"Direction engine error: {e}")
        return metrics

direction_engine = DirectionEngine()


# ============ v7.0 NEW: FULL CONTEXT BUILDER ============
class ContextBuilder:
    """
    Builds a complete MarketContext snapshot at signal time.
    Every variable that MIGHT matter gets logged.
    Edge discovery will find which ones actually do.
    """

    def build(self, now: datetime.datetime, trend_bias: TrendBias, trend_score: float,
              sma_data: Dict, wave: WaveStructure, momentum: MomentumSignals,
              volume_breakout: VolumeBreakout, direction_metrics: DirectionMetrics,
              liquidity_analysis: Dict, df_1h: Optional[pd.DataFrame],
              df_4h: Optional[pd.DataFrame], df_daily: Optional[pd.DataFrame],
              df_weekly: Optional[pd.DataFrame], current_price: float,
              side: str, quality_score: float, quality_tier: str,
              forced_move_prob: str) -> MarketContext:

        ctx = MarketContext()

        # === TIME ===
        ctx.hour_utc = now.hour
        ctx.minute_utc = now.minute
        ctx.day_of_week = now.weekday()
        ctx.week_of_month = (now.day - 1) // 7 + 1
        ctx.month = now.month
        ctx.session = SessionClassifier.classify(now)
        ctx.is_romeo_key_hour = SessionClassifier.is_romeo_key_hour(now)
        ctx.is_high_prob_day = SessionClassifier.is_high_prob_day(now)
        ctx.is_first_week = SessionClassifier.is_first_week(now)

        # === TREND ===
        ctx.trend_bias = trend_bias.value
        ctx.trend_score = trend_score
        ctx.daily_sma50 = sma_data.get('daily_sma50', 0.0)
        ctx.daily_sma200 = sma_data.get('daily_sma200', 0.0)
        ctx.h4_sma50 = sma_data.get('h4_sma50', 0.0)
        ctx.h4_sma200 = sma_data.get('h4_sma200', 0.0)
        ctx.price_vs_daily_sma50_pct = sma_data.get('price_vs_d_sma50_pct', 0.0)
        ctx.price_vs_daily_sma200_pct = sma_data.get('price_vs_d_sma200_pct', 0.0)
        ctx.daily_sma_golden_cross = sma_data.get('daily_golden_cross', False)
        ctx.h4_sma_golden_cross = sma_data.get('h4_golden_cross', False)

        # === WAVE ===
        ctx.wave_pattern = wave.pattern.value
        ctx.wave_confidence = wave.pattern_confidence
        ctx.impulse_size_pct = wave.impulse_size_pct
        ctx.fib_retracement = wave.current_retracement
        ctx.in_optimal_zone = wave.in_optimal_zone
        ctx.distance_to_zone_pct = wave.distance_to_zone_pct
        ctx.fib_500 = wave.fib_500
        ctx.fib_618 = wave.fib_618
        ctx.fib_705 = wave.fib_705

        # === CRT CONTEXT ===
        if df_4h is not None and len(df_4h) >= 3:
            last_candle = df_4h.iloc[-1]
            prev_candle = df_4h.iloc[-2]

            ts_type, body_inside, wick_ext = crt_detector.detect_turtle_soup_type(
                last_candle['high'], last_candle['low'],
                last_candle['open'], last_candle['close'],
                wave.zone_price_high if side == "SELL" else wave.correction_start,
                wave.zone_price_low if side == "BUY" else wave.correction_start,
                side
            )
            ctx.turtle_soup_type = ts_type
            ctx.ts_body_closed_inside = body_inside
            ctx.ts_wick_size_pct = wick_ext
            ctx.inside_bar_count = crt_detector.count_inside_bars(df_4h)
            ctx.csd_confirmed = crt_detector.detect_csd(df_4h, side)
            ctx.atr_14_pct = crt_detector.calculate_atr(df_4h, 14)
            ctx.atr_expanding = crt_detector.is_atr_expanding(df_4h)
            if df_4h is not None and wave.fib_500 > 0:
                candle_range = last_candle['high'] - last_candle['low']
                atr_abs = current_price * ctx.atr_14_pct / 100
                ctx.candle_range_vs_atr = candle_range / atr_abs if atr_abs > 0 else 0.0

        if df_1h is not None:
            asia_high, asia_low = crt_detector.get_asia_range(df_1h, now)
            ctx.asia_range_high = asia_high
            ctx.asia_range_low = asia_low
            if asia_high > 0 and asia_low > 0:
                if side == "SELL" and current_price > asia_high:
                    ctx.london_broke_asia = True
                elif side == "BUY" and current_price < asia_low:
                    ctx.london_broke_asia = True
                tol = (asia_high - asia_low) * 0.1
                ctx.ts_within_asia_range = (
                    (asia_low - tol) <= current_price <= (asia_high + tol)
                )

        if df_daily is not None:
            at_day, at_week, at_session = crt_detector.check_at_key_level(
                current_price, df_daily, df_weekly
            )
            ctx.at_prev_day_hl = at_day
            ctx.at_prev_week_hl = at_week
            ctx.at_session_hl = at_session

        # === MOMENTUM ===
        ctx.divergence_type = momentum.divergence_type.value
        ctx.divergence_strength = momentum.divergence_strength
        ctx.rsi_current = momentum.rsi_current
        ctx.rsi_in_zone = (
            (momentum.rsi_current > 40 and side == "BUY") or
            (momentum.rsi_current < 60 and side == "SELL")
        )
        ctx.macd_crossed = momentum.macd_crossed
        ctx.macd_cross_direction = momentum.macd_cross_direction
        ctx.macd_histogram_reversal = momentum.macd_histogram_reversal
        ctx.momentum_score = momentum.momentum_score
        ctx.momentum_aligned = momentum.momentum_aligned

        # === VOLUME ===
        ctx.volume_triggered = volume_breakout.triggered
        ctx.volume_ratio = volume_breakout.volume_ratio
        ctx.sweep_then_reclaim = volume_breakout.sweep_then_reclaim
        ctx.pattern_break = volume_breakout.pattern_break
        ctx.volume_score = volume_breakout.volume_score

        # === INSTITUTIONAL ===
        ctx.funding_rate = direction_metrics.funding_extreme if direction_metrics.bleeding_side else 0.0
        ctx.funding_extreme = direction_metrics.funding_extreme > 0
        ctx.funding_bleeding_side = direction_metrics.bleeding_side
        ctx.trapped_side = direction_metrics.trapped_side.value
        ctx.trapped_confidence = direction_metrics.trapped_confidence
        ctx.direction_score = direction_metrics.direction_score
        ctx.direction_tier = direction_metrics.confidence_tier.value

        # === LIQUIDITY ===
        pools = liquidity_analysis.get('identified_pools', {})
        ctx.liquidity_pools_count = sum(pools.values()) if pools else 0
        ctx.rr_ratio = liquidity_analysis.get('rr_ratio', 0.0)
        ctx.risk_pct = liquidity_analysis.get('risk_pct', 0.0)

        # === QUALITY ===
        ctx.quality_score = quality_score
        ctx.quality_tier = quality_tier
        ctx.forced_move_probability = forced_move_prob

        return ctx

context_builder = ContextBuilder()


# ============ v7.0 NEW: OUTCOME TRACKER ============
class OutcomeTracker:
    """
    Monitors all active signals and fills in outcome data.
    MAE/MFE tracking — the most valuable data for edge discovery.
    """

    def __init__(self):
        self.active_records: Dict[int, SignalRecord] = {}  # id → record

    def register(self, record: SignalRecord):
        if record.id is not None:
            self.active_records[record.id] = record

    def update_price(self, signal_id: int, current_price: float,
                     current_high: float, current_low: float) -> Optional[Dict]:
        """
        Call this every scan cycle for each active signal.
        Returns outcome dict if signal resolved, else None.
        """
        if signal_id not in self.active_records:
            return None

        record = self.active_records[signal_id]
        entry = record.entry_price
        sl = record.sl_price
        tp1 = record.tp1
        tp2 = record.tp2

        if entry <= 0:
            return None

        # Update MAE/MFE
        if record.side == "BUY":
            adverse = (entry - current_low) / entry * 100
            favorable = (current_high - entry) / entry * 100
        else:
            adverse = (current_high - entry) / entry * 100
            favorable = (entry - current_low) / entry * 100

        record.max_adverse_pct = max(record.max_adverse_pct, max(0, adverse))
        record.max_favorable_pct = max(record.max_favorable_pct, max(0, favorable))

        # Check outcomes
        outcome = None

        if record.side == "BUY":
            if sl > 0 and current_low <= sl and record.outcome_tp1 == "PENDING":
                outcome = self._close(record, current_price, "SL_HIT", -1.0)
            elif tp1 > 0 and current_high >= tp1 and record.outcome_tp1 == "PENDING":
                record.outcome_tp1 = "WIN"
                record.bars_to_tp1 = self._bars_elapsed(record)
                record.actual_rr = abs(tp1 - entry) / abs(entry - sl) if sl > 0 else 0
                if tp2 > 0 and current_high >= tp2 and record.outcome_tp2 == "PENDING":
                    outcome = self._close(record, current_price, "TP2_HIT",
                                         abs(tp2 - entry) / abs(entry - sl) if sl > 0 else 0)
                else:
                    # Partial — TP1 hit, waiting for TP2
                    record.outcome = "TP1_HIT"
        else:
            if sl > 0 and current_high >= sl and record.outcome_tp1 == "PENDING":
                outcome = self._close(record, current_price, "SL_HIT", -1.0)
            elif tp1 > 0 and current_low <= tp1 and record.outcome_tp1 == "PENDING":
                record.outcome_tp1 = "WIN"
                record.bars_to_tp1 = self._bars_elapsed(record)
                record.actual_rr = abs(entry - tp1) / abs(sl - entry) if sl > 0 else 0
                if tp2 > 0 and current_low <= tp2 and record.outcome_tp2 == "PENDING":
                    outcome = self._close(record, current_price, "TP2_HIT",
                                         abs(entry - tp2) / abs(sl - entry) if sl > 0 else 0)
                else:
                    record.outcome = "TP1_HIT"

        return outcome

    def _close(self, record: SignalRecord, price: float, outcome_type: str, rr: float) -> Dict:
        record.status = "closed"
        record.outcome = outcome_type
        record.actual_rr = rr
        record.closed_at = datetime.datetime.utcnow().isoformat()
        record.closed_price = price
        record.pnl_pct = rr * record.risk_pct if hasattr(record, 'risk_pct') else 0.0
        return {
            'signal_id': record.id,
            'outcome': outcome_type,
            'actual_rr': rr,
            'max_adverse_pct': record.max_adverse_pct,
            'max_favorable_pct': record.max_favorable_pct,
            'bars_to_tp1': record.bars_to_tp1,
            'bars_to_sl': record.bars_to_sl,
            'closed_price': price,
            'closed_at': record.closed_at
        }

    def _bars_elapsed(self, record: SignalRecord) -> int:
        try:
            created = datetime.datetime.fromisoformat(record.timestamp)
            delta = (datetime.datetime.utcnow() - created).total_seconds()
            return int(delta / 3600)  # Approximate in hours
        except:
            return 0

    def check_expired(self) -> List[int]:
        now = datetime.datetime.utcnow()
        expired = []
        for sid, record in list(self.active_records.items()):
            if record.status == "active":
                try:
                    created = datetime.datetime.fromisoformat(record.timestamp)
                    age_h = (now - created).total_seconds() / 3600
                    if age_h > SIGNAL_VALIDITY_HOURS:
                        record.status = "closed"
                        record.outcome = "EXPIRED"
                        record.closed_at = now.isoformat()
                        expired.append(sid)
                except:
                    pass
        return expired

outcome_tracker = OutcomeTracker()


# ============ v7.0 NEW: EDGE DISCOVERY ENGINE ============
class EdgeDiscoveryEngine:
    """
    Runs statistical analysis on resolved signals.
    Finds which conditions actually produce positive expectancy.
    Reports via Telegram and logs findings.
    """

    def __init__(self, min_sample: int = 30, min_winrate: float = 0.60):
        self.min_sample = min_sample
        self.min_winrate = min_winrate

    async def run_analysis(self, db_conn) -> Dict:
        try:
            async with db_conn.execute(
                "SELECT * FROM signals_v7 WHERE status != 'active' AND outcome != 'PENDING'"
            ) as cursor:
                rows = await cursor.fetchall()
                if not rows:
                    return {}
                cols = [d[0] for d in cursor.description]

            df = pd.DataFrame(rows, columns=cols)
            if len(df) < self.min_sample:
                log.info(f"Edge discovery: only {len(df)} resolved signals, need {self.min_sample}")
                return {}

            df['won_tp1'] = (df['outcome_tp1'] == 'WIN').astype(int)
            df['won_any'] = df['outcome'].isin(['TP1_HIT', 'TP2_HIT', 'TP3_HIT']).astype(int)
            df['actual_rr'] = pd.to_numeric(df['actual_rr'], errors='coerce').fillna(0)

            results = {
                'total_signals': len(df),
                'resolved': len(df[df['status'] == 'closed']),
                'overall_winrate': df['won_tp1'].mean() * 100,
                'overall_avg_rr': df['actual_rr'].mean(),
                'overall_expectancy': self._expectancy(df['actual_rr']),
                'best_conditions': [],
                'worst_conditions': [],
                'best_hours': [],
                'best_days': [],
                'optimal_rr_threshold': self._find_optimal_rr(df),
            }

            # Single variable analysis
            binary_vars = [
                'is_romeo_key_hour', 'is_high_prob_day', 'in_optimal_zone',
                'ts_body_closed_inside', 'csd_confirmed', 'c3_entry',
                'at_prev_day_hl', 'at_prev_week_hl', 'london_broke_asia',
                'macd_crossed', 'momentum_aligned', 'volume_triggered',
                'sweep_then_reclaim', 'funding_extreme', 'momentum_aligned'
            ]

            best = []
            worst = []
            for var in binary_vars:
                if var not in df.columns:
                    continue
                for val in [0, 1]:
                    subset = df[df[var] == val]
                    if len(subset) < self.min_sample:
                        continue
                    wr = subset['won_tp1'].mean()
                    avg_rr = subset['actual_rr'].mean()
                    exp = self._expectancy(subset['actual_rr'])
                    try:
                        _, p = scipy_stats.ttest_1samp(subset['actual_rr'].dropna(), 0)
                    except:
                        p = 1.0
                    entry = {
                        'variable': f"{var}={val}",
                        'win_rate_pct': round(wr * 100, 1),
                        'sample_n': len(subset),
                        'avg_rr': round(avg_rr, 2),
                        'expectancy': round(exp, 3),
                        'p_value': round(float(p), 4),
                        'significant': bool(p < 0.05 and len(subset) >= self.min_sample)
                    }
                    if wr >= self.min_winrate and p < 0.05:
                        best.append(entry)
                    elif wr < 0.40 and p < 0.05:
                        worst.append(entry)

            results['best_conditions'] = sorted(best, key=lambda x: x['expectancy'], reverse=True)[:10]
            results['worst_conditions'] = sorted(worst, key=lambda x: x['win_rate_pct'])[:10]

            # Time analysis
            hour_stats = df.groupby('hour_utc').apply(
                lambda g: pd.Series({
                    'win_rate': g['won_tp1'].mean() * 100,
                    'count': len(g),
                    'avg_rr': g['actual_rr'].mean(),
                    'expectancy': self._expectancy(g['actual_rr'])
                })
            ).reset_index()
            results['best_hours'] = hour_stats[hour_stats['count'] >= 10].sort_values(
                'expectancy', ascending=False
            ).head(5).to_dict('records')

            day_stats = df.groupby('day_of_week').apply(
                lambda g: pd.Series({
                    'day_name': ['Mon','Tue','Wed','Thu','Fri','Sat','Sun'][int(g['day_of_week'].iloc[0])],
                    'win_rate': g['won_tp1'].mean() * 100,
                    'count': len(g),
                    'avg_rr': g['actual_rr'].mean()
                })
            ).reset_index()
            results['best_days'] = day_stats[day_stats['count'] >= 5].sort_values(
                'win_rate', ascending=False
            ).to_dict('records')

            log.info(f"Edge Discovery Complete: {len(df)} signals, WR={results['overall_winrate']:.1f}%, "
                     f"Found {len(results['best_conditions'])} winning conditions")

            return results

        except Exception as e:
            log.error(f"Edge discovery error: {e}")
            import traceback
            log.error(traceback.format_exc())
            return {}

    def _expectancy(self, rr_series) -> float:
        rr = rr_series.dropna()
        if len(rr) == 0:
            return 0.0
        wins = rr[rr > 0]
        losses = rr[rr < 0]
        wr = len(wins) / len(rr)
        avg_win = wins.mean() if len(wins) > 0 else 0
        avg_loss = abs(losses.mean()) if len(losses) > 0 else 0
        return round(wr * avg_win - (1 - wr) * avg_loss, 3)

    def _find_optimal_rr(self, df: pd.DataFrame) -> Dict:
        """Find the RR threshold that maximizes expectancy"""
        best_exp = -999
        best_rr = 1.0
        for min_rr in [1.0, 1.5, 2.0, 2.5, 3.0]:
            subset = df[df['rr_ratio'] >= min_rr]
            if len(subset) < self.min_sample:
                continue
            exp = self._expectancy(subset['actual_rr'])
            if exp > best_exp:
                best_exp = exp
                best_rr = min_rr
        return {'optimal_min_rr': best_rr, 'expectancy_at_optimal': best_exp,
                'sample_at_optimal': len(df[df['rr_ratio'] >= best_rr])}

edge_engine = EdgeDiscoveryEngine()


# ============ TELEGRAM ============
async def send_telegram(msg: str, parse_mode="HTML"):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            await client.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": msg,
                                          "parse_mode": parse_mode, "disable_web_page_preview": True})
        except Exception as e:
            log.warning(f"Telegram send failed: {e}")


# ============ ALERT FORMATTING (upgraded for v7.0) ============
async def send_v7_alert(record: SignalRecord, ctx: MarketContext):
    try:
        qs = ctx.quality_score
        if qs >= 4.5 and ctx.momentum_aligned and ctx.volume_triggered:
            tier_label, tier_emoji = "S+", "🔮"
        elif qs >= 3.5 and ctx.volume_triggered:
            tier_label, tier_emoji = "A+", "🔥"
        elif qs >= 2.5:
            tier_label, tier_emoji = "A", "✅"
        elif qs >= 2.0:
            tier_label, tier_emoji = "B", "⚠️"
        else:
            tier_label, tier_emoji = "C", "📊"

        ts_emoji = "🟢 TBS" if ctx.turtle_soup_type == TurtleSoupType.TBS else "🟡 TWS" if ctx.turtle_soup_type == TurtleSoupType.TWS else "⚪ N/A"
        csd_emoji = "✅" if ctx.csd_confirmed else "⏳"
        session_emoji = {"ASIA": "🌙", "LONDON": "🇬🇧", "NEW_YORK": "🗽", "OVERLAP": "⚡", "OFF": "😴"}.get(ctx.session.value, "")
        key_time_emoji = "⭐" if ctx.is_romeo_key_hour else ""

        tp_lines = []
        for i, tp in enumerate([record.tp1, record.tp2, record.tp3]):
            if tp and tp > 0 and record.entry_price > 0:
                dist = abs(tp - record.entry_price) / record.entry_price * 100
                tp_lines.append(f"TP{i+1}: <code>{tp:.8f}</code> ({dist:.1f}%)")

        msg = f"""{tier_emoji} <b>ROMEOTPT v7.0 — {record.symbol} | {record.side}</b>
<b>Tier: {tier_label} | Quality: {qs:.1f}/5.0</b>

<b>📐 WAVE:</b> {ctx.wave_pattern} ({ctx.wave_confidence:.0%}) | Retrace: {ctx.fib_retracement:.0%}
{'📍 IN OPTIMAL ZONE ✅' if ctx.in_optimal_zone else f'📍 {ctx.distance_to_zone_pct:.1f}% from zone'}

<b>🕯️ CRT CONTEXT:</b>
{ts_emoji} | CSD: {csd_emoji} | Inside bars: {ctx.inside_bar_count}
{'🌏 London broke Asia range ✅' if ctx.london_broke_asia else ''}
{'📅 At prev day H/L ✅' if ctx.at_prev_day_hl else ''}{'📅 At prev week H/L ✅' if ctx.at_prev_week_hl else ''}

<b>📈 MOMENTUM:</b> {ctx.divergence_type} ({ctx.divergence_strength:.0%}) | RSI: {ctx.rsi_current:.1f}
MACD: {'✅ ' + ctx.macd_cross_direction if ctx.macd_crossed else '⏳'} | Score: {ctx.momentum_score:.0%}
{'🎯 ALIGNED ✅' if ctx.momentum_aligned else ''}

<b>📊 VOLUME:</b> {'🚀 ' + str(round(ctx.volume_ratio, 1)) + 'x avg' if ctx.volume_triggered else '⏳ No breakout'}
{'🧹 SWEEP + RECLAIM ✅' if ctx.sweep_then_reclaim else ''}

<b>🕐 TIME:</b> {session_emoji} {ctx.session.value} {key_time_emoji}
{'⭐ ROMEO KEY HOUR' if ctx.is_romeo_key_hour else ''} | Day: {'✅ High prob' if ctx.is_high_prob_day else '⚠️ Lower prob'}

<b>🎯 TRADE:</b>
Entry: <code>{record.entry_price:.8f}</code>
{chr(10).join(tp_lines)}
🛡️ SL: <code>{record.sl_price:.8f}</code>
⚖️ RR: <b>{record.rr_ratio:.1f}:1</b> | Risk: {ctx.risk_pct:.2f}%

<b>📊 FORCED MOVE PROB: {ctx.forced_move_probability}</b>
<i>Signal ID: {record.id} | v7.0 Harvester | {datetime.datetime.utcnow().strftime('%H:%M:%S UTC')}</i>
"""
        await send_telegram(msg)
    except Exception as e:
        log.error(f"Alert formatting error: {e}")


async def send_edge_discovery_report(results: Dict):
    try:
        if not results:
            return
        best = results.get('best_conditions', [])
        worst = results.get('worst_conditions', [])

        best_lines = "\n".join([
            f"• {c['variable']}: WR={c['win_rate_pct']}% n={c['sample_n']} E={c['expectancy']}"
            for c in best[:5]
        ]) or "None found yet"

        worst_lines = "\n".join([
            f"• {c['variable']}: WR={c['win_rate_pct']}% n={c['sample_n']}"
            for c in worst[:5]
        ]) or "None found yet"

        best_hours = results.get('best_hours', [])
        hour_lines = "\n".join([
            f"• {int(h['hour_utc'])}:00 UTC: WR={h['win_rate']:.1f}% n={int(h['count'])}"
            for h in best_hours[:5]
        ]) or "Need more data"

        optimal_rr = results.get('optimal_rr_threshold', {})
        optimal_line = (f"Optimal min RR: {optimal_rr.get('optimal_min_rr', '?')}x "
                       f"(E={optimal_rr.get('expectancy_at_optimal', '?')}, "
                       f"n={optimal_rr.get('sample_at_optimal', '?')})")

        msg = f"""🔬 <b>EDGE DISCOVERY REPORT — v7.0</b>

📊 <b>Overview:</b>
Total signals: {results.get('total_signals', 0)}
Resolved: {results.get('resolved', 0)}
Overall WR: {results.get('overall_winrate', 0):.1f}%
Avg RR: {results.get('overall_avg_rr', 0):.2f}
Expectancy: {results.get('overall_expectancy', 0):.3f}

✅ <b>Best Conditions (p&lt;0.05):</b>
{best_lines}

❌ <b>Conditions to Avoid:</b>
{worst_lines}

🕐 <b>Best Hours (UTC):</b>
{hour_lines}

🎯 <b>{optimal_line}</b>

<i>Run automatically every 100 new resolved signals</i>
"""
        await send_telegram(msg)
    except Exception as e:
        log.error(f"Edge report send error: {e}")


# ============ DATABASE ============
async def init_database(db_conn):
    try:
        await db_conn.execute("""
            CREATE TABLE IF NOT EXISTS signals_v7 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,

                -- Identity
                symbol TEXT,
                side TEXT,
                timestamp TEXT,
                current_price REAL,
                entry_price REAL,
                entry_type TEXT,
                sl_price REAL,
                tp1 REAL, tp2 REAL, tp3 REAL,
                sl_pips_pct REAL,
                tp1_pips_pct REAL,
                tp2_pips_pct REAL,
                modeled_rr_tp1 REAL,
                modeled_rr_tp2 REAL,

                -- Key flattened context (for SQL queries)
                hour_utc INTEGER,
                day_of_week INTEGER,
                session TEXT,
                is_romeo_key_hour INTEGER,
                is_high_prob_day INTEGER,
                trend_bias TEXT,
                wave_pattern TEXT,
                wave_confidence REAL,
                fib_retracement REAL,
                in_optimal_zone INTEGER,
                turtle_soup_type TEXT,
                ts_body_closed_inside INTEGER,
                csd_confirmed INTEGER,
                c3_entry INTEGER,
                at_prev_day_hl INTEGER,
                at_prev_week_hl INTEGER,
                london_broke_asia INTEGER,
                inside_bar_count INTEGER,
                divergence_type TEXT,
                divergence_strength REAL,
                rsi_current REAL,
                macd_crossed INTEGER,
                momentum_aligned INTEGER,
                momentum_score REAL,
                volume_triggered INTEGER,
                volume_ratio REAL,
                sweep_then_reclaim INTEGER,
                funding_rate REAL,
                funding_extreme INTEGER,
                oi_change_24h REAL,
                trapped_side TEXT,
                direction_score REAL,
                direction_tier TEXT,
                atr_14_pct REAL,
                quality_score REAL,
                quality_tier TEXT,
                rr_ratio REAL,

                -- Full context JSON (everything, for future analysis)
                context_json TEXT,

                -- Outcome (filled by OutcomeTracker)
                status TEXT DEFAULT 'active',
                outcome TEXT DEFAULT 'active',
                outcome_tp1 TEXT DEFAULT 'PENDING',
                outcome_tp2 TEXT DEFAULT 'PENDING',
                outcome_tp3 TEXT DEFAULT 'PENDING',
                actual_rr REAL DEFAULT 0,
                bars_to_tp1 INTEGER DEFAULT 0,
                bars_to_sl INTEGER DEFAULT 0,
                max_adverse_pct REAL DEFAULT 0,
                max_favorable_pct REAL DEFAULT 0,
                closed_at TEXT,
                closed_price REAL,
                pnl_pct REAL DEFAULT 0,
                alert_sent INTEGER DEFAULT 0,

                UNIQUE(symbol, side, timestamp)
            )
        """)
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_v7_status ON signals_v7 (status)")
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_v7_symbol ON signals_v7 (symbol)")
        await db_conn.execute("CREATE INDEX IF NOT EXISTS idx_v7_outcome ON signals_v7 (outcome)")
        await db_conn.commit()
        log.info("✅ Database v7.0 initialized — Full harvester schema ready")
    except Exception as e:
        log.error(f"Database init error: {e}")


db_lock = asyncio.Lock()
db_conn = None
signal_id_map: Dict[str, int] = {}  # "symbol_side_timestamp" → db id


async def store_signal(record: SignalRecord) -> Optional[int]:
    """Store new signal, return DB id"""
    async with db_lock:
        try:
            ctx = json.loads(record.context_json) if record.context_json else {}
            cursor = await db_conn.execute("""
                INSERT OR IGNORE INTO signals_v7 (
                    symbol, side, timestamp, current_price, entry_price, entry_type,
                    sl_price, tp1, tp2, tp3, sl_pips_pct, tp1_pips_pct, tp2_pips_pct,
                    modeled_rr_tp1, modeled_rr_tp2,
                    hour_utc, day_of_week, session, is_romeo_key_hour, is_high_prob_day,
                    trend_bias, wave_pattern, wave_confidence, fib_retracement, in_optimal_zone,
                    turtle_soup_type, ts_body_closed_inside, csd_confirmed, c3_entry,
                    at_prev_day_hl, at_prev_week_hl, london_broke_asia, inside_bar_count,
                    divergence_type, divergence_strength, rsi_current, macd_crossed,
                    momentum_aligned, momentum_score, volume_triggered, volume_ratio,
                    sweep_then_reclaim, funding_rate, funding_extreme, oi_change_24h,
                    trapped_side, direction_score, direction_tier, atr_14_pct,
                    quality_score, quality_tier, rr_ratio, context_json, alert_sent
                ) VALUES (
                    ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
                    ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
                )
            """, (
                record.symbol, record.side, record.timestamp,
                float(record.current_price), float(record.entry_price), record.entry_type,
                float(record.sl_price),
                float(record.tp1) if record.tp1 else None,
                float(record.tp2) if record.tp2 else None,
                float(record.tp3) if record.tp3 else None,
                float(record.sl_pips_pct), float(record.tp1_pips_pct),
                float(record.tp2_pips_pct), float(record.modeled_rr_tp1), float(record.modeled_rr_tp2),
                record.hour_utc, record.day_of_week, record.session,
                1 if record.is_romeo_key_hour else 0,
                1 if record.is_high_prob_day else 0,
                record.trend_bias, record.wave_pattern, float(record.wave_confidence),
                float(record.fib_retracement), 1 if record.in_optimal_zone else 0,
                record.turtle_soup_type, 1 if record.ts_body_closed_inside else 0,
                1 if record.csd_confirmed else 0, 1 if record.c3_entry else 0,
                1 if record.at_prev_day_hl else 0, 1 if record.at_prev_week_hl else 0,
                1 if record.london_broke_asia else 0,
                ctx.get('inside_bar_count', 0),
                record.divergence_type, float(record.divergence_strength),
                float(record.rsi_current), 1 if record.macd_crossed else 0,
                1 if record.momentum_aligned else 0, float(record.momentum_score),
                1 if record.volume_triggered else 0, float(record.volume_ratio),
                1 if record.sweep_then_reclaim else 0, float(record.funding_rate),
                1 if record.funding_extreme else 0, float(record.oi_change_24h),
                record.trapped_side, float(record.direction_score), record.direction_tier,
                float(record.atr_14_pct), float(record.quality_score),
                record.quality_tier, float(record.rr_ratio),
                record.context_json, record.alert_sent
            ))
            await db_conn.commit()
            return cursor.lastrowid
        except Exception as e:
            log.error(f"Error storing signal: {e}")
            return None


async def update_signal_outcome(signal_id: int, outcome_data: Dict):
    """Update a signal record with outcome data"""
    async with db_lock:
        try:
            await db_conn.execute("""
                UPDATE signals_v7 SET
                    status=?, outcome=?, outcome_tp1=?, outcome_tp2=?, outcome_tp3=?,
                    actual_rr=?, bars_to_tp1=?, bars_to_sl=?,
                    max_adverse_pct=?, max_favorable_pct=?,
                    closed_at=?, closed_price=?, pnl_pct=?
                WHERE id=?
            """, (
                outcome_data.get('status', 'closed'),
                outcome_data.get('outcome', 'unknown'),
                outcome_data.get('outcome_tp1', 'PENDING'),
                outcome_data.get('outcome_tp2', 'PENDING'),
                outcome_data.get('outcome_tp3', 'PENDING'),
                float(outcome_data.get('actual_rr', 0)),
                int(outcome_data.get('bars_to_tp1', 0)),
                int(outcome_data.get('bars_to_sl', 0)),
                float(outcome_data.get('max_adverse_pct', 0)),
                float(outcome_data.get('max_favorable_pct', 0)),
                outcome_data.get('closed_at', ''),
                float(outcome_data.get('closed_price', 0)),
                float(outcome_data.get('pnl_pct', 0)),
                signal_id
            ))
            await db_conn.commit()
        except Exception as e:
            log.error(f"Error updating outcome for signal {signal_id}: {e}")


# ============ DEDUPLICATION (improved in v7.0) ============
active_signal_keys: Dict[str, Dict] = {}  # "symbol_side" → {timestamp, score}

def should_send_alert(symbol: str, side: str, quality_score: float) -> bool:
    key = f"{symbol}_{side}"
    now = datetime.datetime.utcnow()
    if key in active_signal_keys:
        last = active_signal_keys[key]
        age_m = (now - last['timestamp']).total_seconds() / 60
        if age_m < SIGNAL_COOLDOWN_MINUTES:
            return False
        if age_m < SIGNAL_VALIDITY_HOURS * 60:
            # Allow re-alert only if score improved significantly
            if quality_score <= last['score'] + 0.5:
                return False
    return True


# ============ MAIN SCANNER v7.0 ============
async def scan_symbol_v7(exchange, symbol: str) -> Optional[Tuple[SignalRecord, MarketContext]]:
    """
    v7.0: Same detection logic as v6.0 but:
    1. Accepts ALL signals above a very low floor (0.5 quality)
    2. Captures 50+ variable context snapshot
    3. Returns SignalRecord + MarketContext for logging
    """
    try:
        df_daily = create_dataframe(await fetch_ohlcv(exchange, symbol, "1d", 200))
        df_4h = create_dataframe(await fetch_ohlcv(exchange, symbol, "4h", 100))
        df_1h = create_dataframe(await fetch_ohlcv(exchange, symbol, "1h", 100))
        df_15m = create_dataframe(await fetch_ohlcv(exchange, symbol, "15m", 100))
        df_5m = create_dataframe(await fetch_ohlcv(exchange, symbol, "5m", 50))
        df_weekly = create_dataframe(await fetch_ohlcv(exchange, symbol, "1w", 52))

        if df_daily is None or df_4h is None or df_1h is None:
            return None

        ticker = await safe_fetch_ticker(exchange, symbol)
        if not ticker:
            return None
        current_price = ticker.get('last', 0)
        if current_price <= 0:
            return None

        now = datetime.datetime.utcnow()

        # ── STEP 1: TREND BIAS ──
        trend_bias, trend_score, sma_data = wave_detector.detect_trend_bias(df_daily, df_4h)
        if trend_bias == TrendBias.NEUTRAL:
            return None

        # ── STEP 2: WAVE STRUCTURE ──
        wave = wave_detector.identify_abc_correction(df_4h, trend_bias)
        if wave.pattern == WavePattern.NONE or wave.pattern_confidence < 0.2:
            return None

        # ── STEP 3: MOMENTUM ──
        momentum = momentum_engine.analyze_momentum(df_1h, df_15m, trend_bias, wave)

        # v7.0: Accept even low momentum — log everything
        if momentum.divergence_type == DivergenceType.NONE and momentum.momentum_score < 0.2:
            return None  # Only reject absolute zero momentum

        # ── STEP 4: SIDE & ENTRY ──
        side = "BUY" if trend_bias == TrendBias.BULLISH else "SELL"
        entry_price = current_price
        entry_type = "DISCOUNT_FIB_ZONE" if side == "BUY" else "PREMIUM_FIB_ZONE"

        # ── STEP 5: VOLUME ──
        volume_breakout = volume_trigger.detect_breakout(df_5m, df_15m, trend_bias, wave, entry_price)

        # ── STEP 6: TP/SL FROM LIQUIDITY ──
        sl_price, tp_targets, tp_sources, liquidity_analysis = await calculate_liquidity_tp_sl(
            exchange, symbol, side, entry_price, entry_type
        )
        if sl_price <= 0 or not tp_targets:
            return None

        risk = abs(entry_price - sl_price)
        reward = abs(tp_targets[0] - entry_price) if tp_targets else 0
        rr_ratio = reward / risk if risk > 0 else 0

        if rr_ratio < 1.0:  # v7.0: lower floor, collect more data
            return None

        # ── STEP 7: DIRECTION (confluence) ──
        direction_metrics = await direction_engine.analyze_direction(
            exchange, symbol, side, current_price
        )

        # ── STEP 8: QUALITY SCORE ──
        quality_score = 0.0
        quality_score += wave.pattern_confidence * 1.5
        quality_score += momentum.momentum_score * 1.5
        quality_score += volume_breakout.volume_score * 1.0
        if wave.in_optimal_zone:
            quality_score += 0.5
        if direction_metrics.confidence_tier == DirectionTier.HIGH:
            quality_score += 0.5
        elif direction_metrics.confidence_tier == DirectionTier.MEDIUM:
            quality_score += 0.3

        if quality_score >= 4.5: quality_tier = "S+"
        elif quality_score >= 4.0: quality_tier = "A+"
        elif quality_score >= 3.0: quality_tier = "A"
        elif quality_score >= 2.5: quality_tier = "B"
        else: quality_tier = "C"

        # v7.0: Accept everything above floor — NO filtering by tier
        if quality_score < MIN_QUALITY_SCORE:
            return None

        forced_move_prob = (
            "HIGH" if (wave.in_optimal_zone and momentum.momentum_aligned and volume_breakout.triggered and quality_score >= 3.5)
            else "MODERATE" if (momentum.momentum_aligned and quality_score >= 2.5)
            else "LOW"
        )

        # ── BUILD FULL CONTEXT SNAPSHOT ──
        ctx = context_builder.build(
            now=now,
            trend_bias=trend_bias,
            trend_score=trend_score,
            sma_data=sma_data,
            wave=wave,
            momentum=momentum,
            volume_breakout=volume_breakout,
            direction_metrics=direction_metrics,
            liquidity_analysis=liquidity_analysis,
            df_1h=df_1h,
            df_4h=df_4h,
            df_daily=df_daily,
            df_weekly=df_weekly,
            current_price=current_price,
            side=side,
            quality_score=quality_score,
            quality_tier=quality_tier,
            forced_move_prob=forced_move_prob
        )

        # ── BUILD SIGNAL RECORD ──
        tp1 = tp_targets[0] if len(tp_targets) > 0 else 0.0
        tp2 = tp_targets[1] if len(tp_targets) > 1 else 0.0
        tp3 = tp_targets[2] if len(tp_targets) > 2 else 0.0

        sl_pct = abs(entry_price - sl_price) / entry_price * 100 if entry_price > 0 else 0
        tp1_pct = abs(tp1 - entry_price) / entry_price * 100 if entry_price > 0 and tp1 > 0 else 0
        tp2_pct = abs(tp2 - entry_price) / entry_price * 100 if entry_price > 0 and tp2 > 0 else 0

        record = SignalRecord(
            symbol=symbol,
            timestamp=now.isoformat(),
            side=side,
            current_price=current_price,
            entry_price=entry_price,
            entry_type=entry_type,
            sl_price=sl_price,
            tp1=tp1, tp2=tp2, tp3=tp3,
            sl_pips_pct=sl_pct,
            tp1_pips_pct=tp1_pct,
            tp2_pips_pct=tp2_pct,
            modeled_rr_tp1=rr_ratio,
            modeled_rr_tp2=(abs(tp2 - entry_price) / risk) if tp2 > 0 and risk > 0 else 0,

            # Flattened context
            hour_utc=ctx.hour_utc,
            day_of_week=ctx.day_of_week,
            session=ctx.session.value,
            is_romeo_key_hour=ctx.is_romeo_key_hour,
            is_high_prob_day=ctx.is_high_prob_day,
            trend_bias=ctx.trend_bias,
            wave_pattern=ctx.wave_pattern,
            wave_confidence=ctx.wave_confidence,
            fib_retracement=ctx.fib_retracement,
            in_optimal_zone=ctx.in_optimal_zone,
            turtle_soup_type=ctx.turtle_soup_type.value,
            ts_body_closed_inside=ctx.ts_body_closed_inside,
            csd_confirmed=ctx.csd_confirmed,
            c3_entry=ctx.c3_entry,
            at_prev_day_hl=ctx.at_prev_day_hl,
            at_prev_week_hl=ctx.at_prev_week_hl,
            london_broke_asia=ctx.london_broke_asia,
            divergence_type=ctx.divergence_type,
            divergence_strength=ctx.divergence_strength,
            rsi_current=ctx.rsi_current,
            macd_crossed=ctx.macd_crossed,
            momentum_aligned=ctx.momentum_aligned,
            momentum_score=ctx.momentum_score,
            volume_triggered=ctx.volume_triggered,
            volume_ratio=ctx.volume_ratio,
            sweep_then_reclaim=ctx.sweep_then_reclaim,
            funding_rate=ctx.funding_rate,
            funding_extreme=ctx.funding_extreme,
            oi_change_24h=ctx.oi_change_24h if hasattr(ctx, 'oi_change_24h') else 0.0,
            trapped_side=ctx.trapped_side,
            direction_score=ctx.direction_score,
            direction_tier=ctx.direction_tier,
            atr_14_pct=ctx.atr_14_pct,
            quality_score=quality_score,
            quality_tier=quality_tier,
            rr_ratio=rr_ratio,

            # Full JSON blob
            context_json=json.dumps({
                k: (v.value if hasattr(v, 'value') else v)
                for k, v in ctx.__dict__.items()
            }, default=str),

            status="active",
            outcome="active",
            outcome_tp1="PENDING",
            outcome_tp2="PENDING",
            outcome_tp3="PENDING",
        )

        return record, ctx

    except Exception as e:
        log.error(f"v7 scanner error for {symbol}: {e}")
        import traceback
        log.error(traceback.format_exc())
        return None


# ============ MAIN LOOP ============
resolved_since_last_edge_run = 0

async def v7_scanner_main(exchange):
    global resolved_since_last_edge_run

    startup_msg = f"""🚀 <b>ROMEOTPT v7.0 — SIGNAL HARVESTER MODE</b>
Scan: {SCAN_INTERVAL}s | Top {TOP_N} symbols
<b>MODE: Accept all signals, log everything, discover real edge</b>
Min quality floor: {MIN_QUALITY_SCORE} | Min RR: 1.0
<b>50+ variables logged per signal. Edge discovery at {EDGE_DISCOVERY_MIN_SIGNALS} resolved signals.</b>"""
    await send_telegram(startup_msg)

    scan_cycle = 0

    while True:
        scan_cycle += 1
        try:
            tickers = await safe_fetch_tickers(exchange)
            usdt_pairs = [
                (sym, float(data.get("quoteVolume", 0)))
                for sym, data in tickers.items()
                if sym.endswith("/USDT") and not sym.startswith("USDT")
                and isinstance(data.get("quoteVolume"), (int, float))
                and data.get("quoteVolume", 0) > 100000
            ]
            usdt_pairs.sort(key=lambda x: x[1], reverse=True)
            symbols_to_scan = [s[0] for s in usdt_pairs[:TOP_N]]

            # ── OUTCOME TRACKING: Update all active signals ──
            for sig_id, record in list(outcome_tracker.active_records.items()):
                if record.status != "active":
                    continue
                ticker = tickers.get(record.symbol)
                if ticker:
                    current = ticker.get('last', 0)
                    high = ticker.get('high', current)
                    low = ticker.get('low', current)
                    outcome = outcome_tracker.update_price(sig_id, current, high, low)
                    if outcome:
                        await update_signal_outcome(sig_id, {
                            'status': 'closed',
                            'outcome': outcome['outcome'],
                            'outcome_tp1': record.outcome_tp1,
                            'outcome_tp2': record.outcome_tp2,
                            'outcome_tp3': record.outcome_tp3,
                            'actual_rr': outcome['actual_rr'],
                            'bars_to_tp1': outcome.get('bars_to_tp1', 0),
                            'bars_to_sl': outcome.get('bars_to_sl', 0),
                            'max_adverse_pct': outcome['max_adverse_pct'],
                            'max_favorable_pct': outcome['max_favorable_pct'],
                            'closed_at': outcome['closed_at'],
                            'closed_price': outcome['closed_price'],
                            'pnl_pct': 0.0
                        })
                        resolved_since_last_edge_run += 1
                        log.info(f"✅ Outcome: {record.symbol} {record.side} → {outcome['outcome']} RR={outcome['actual_rr']:.2f}")
                        outcome_tracker.active_records.pop(sig_id, None)

            # ── EXPIRED SIGNALS ──
            expired = outcome_tracker.check_expired()
            for eid in expired:
                await update_signal_outcome(eid, {
                    'status': 'closed', 'outcome': 'EXPIRED',
                    'outcome_tp1': 'PENDING', 'outcome_tp2': 'PENDING', 'outcome_tp3': 'PENDING',
                    'actual_rr': 0, 'bars_to_tp1': 0, 'bars_to_sl': 0,
                    'max_adverse_pct': outcome_tracker.active_records.get(eid, SignalRecord()).max_adverse_pct,
                    'max_favorable_pct': outcome_tracker.active_records.get(eid, SignalRecord()).max_favorable_pct,
                    'closed_at': datetime.datetime.utcnow().isoformat(),
                    'closed_price': 0, 'pnl_pct': 0
                })
                resolved_since_last_edge_run += 1

            # ── EDGE DISCOVERY (trigger after enough resolved signals) ──
            if resolved_since_last_edge_run >= EDGE_DISCOVERY_MIN_SIGNALS:
                log.info("🔬 Running edge discovery analysis...")
                results = await edge_engine.run_analysis(db_conn)
                if results:
                    await send_edge_discovery_report(results)
                resolved_since_last_edge_run = 0

            # ── SCAN NEW SIGNALS ──
            log.info(f"🔄 v7.0 Scan #{scan_cycle}: {len(symbols_to_scan)} symbols | "
                     f"Active: {len(outcome_tracker.active_records)}")

            tasks = []
            for symbol in symbols_to_scan:
                tasks.append(asyncio.create_task(scan_symbol_v7(exchange, symbol)))
                if len(tasks) >= 3:
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    for result in results:
                        if isinstance(result, Exception) or result is None:
                            continue
                        record, ctx = result
                        db_id = await store_signal(record)
                        if db_id:
                            record.id = db_id
                            outcome_tracker.register(record)
                            if should_send_alert(record.symbol, record.side, record.quality_score):
                                record.alert_sent = 1
                                await send_v7_alert(record, ctx)
                                active_signal_keys[f"{record.symbol}_{record.side}"] = {
                                    'timestamp': datetime.datetime.utcnow(),
                                    'score': record.quality_score
                                }
                    tasks = []
                    await asyncio.sleep(0.3)

            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, Exception) or result is None:
                        continue
                    record, ctx = result
                    db_id = await store_signal(record)
                    if db_id:
                        record.id = db_id
                        outcome_tracker.register(record)
                        if should_send_alert(record.symbol, record.side, record.quality_score):
                            record.alert_sent = 1
                            await send_v7_alert(record, ctx)
                            active_signal_keys[f"{record.symbol}_{record.side}"] = {
                                'timestamp': datetime.datetime.utcnow(),
                                'score': record.quality_score
                            }

            # Log every 5 scans
            if scan_cycle % 5 == 0:
                async with db_conn.execute("SELECT COUNT(*) FROM signals_v7") as c:
                    total = (await c.fetchone())[0]
                async with db_conn.execute("SELECT COUNT(*) FROM signals_v7 WHERE status='closed'") as c:
                    resolved = (await c.fetchone())[0]
                async with db_conn.execute(
                    "SELECT AVG(CASE WHEN outcome_tp1='WIN' THEN 1.0 ELSE 0.0 END) FROM signals_v7 WHERE status='closed'"
                ) as c:
                    row = await c.fetchone()
                    wr = (row[0] or 0) * 100
                log.info(f"📊 DB: {total} total | {resolved} resolved | WR={wr:.1f}% | "
                         f"Active: {len(outcome_tracker.active_records)}")

            await asyncio.sleep(SCAN_INTERVAL)

        except Exception as e:
            log.error(f"Scanner error: {e}")
            await asyncio.sleep(SCAN_INTERVAL * 2)


# ============ FASTAPI ============
app = FastAPI()

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "version": "7.0 - SIGNAL HARVESTER + EDGE DISCOVERY",
        "active_signals": len(outcome_tracker.active_records),
        "resolved_since_last_discovery": resolved_since_last_edge_run
    }

@app.get("/signals/active")
async def get_active_signals():
    active = []
    for sig_id, record in outcome_tracker.active_records.items():
        if record.status == "active":
            active.append({
                "id": sig_id,
                "symbol": record.symbol,
                "side": record.side,
                "quality_score": record.quality_score,
                "quality_tier": record.quality_tier,
                "trend_bias": record.trend_bias,
                "entry_price": record.entry_price,
                "sl_price": record.sl_price,
                "tp1": record.tp1,
                "rr_ratio": record.rr_ratio,
                "turtle_soup_type": record.turtle_soup_type,
                "csd_confirmed": record.csd_confirmed,
                "is_romeo_key_hour": record.is_romeo_key_hour,
                "max_adverse_pct": record.max_adverse_pct,
                "max_favorable_pct": record.max_favorable_pct,
                "timestamp": record.timestamp
            })
    return {"active_signals": active, "count": len(active)}

@app.get("/signals/stats")
async def get_stats():
    try:
        async with db_conn.execute("SELECT COUNT(*) FROM signals_v7") as c:
            total = (await c.fetchone())[0]
        async with db_conn.execute("SELECT COUNT(*) FROM signals_v7 WHERE status='closed'") as c:
            resolved = (await c.fetchone())[0]
        async with db_conn.execute("""
            SELECT
                AVG(CASE WHEN outcome_tp1='WIN' THEN 1.0 ELSE 0.0 END) as wr,
                AVG(actual_rr) as avg_rr,
                AVG(max_adverse_pct) as avg_mae,
                AVG(max_favorable_pct) as avg_mfe
            FROM signals_v7 WHERE status='closed'
        """) as c:
            row = await c.fetchone()
        return {
            "total_signals": total,
            "resolved_signals": resolved,
            "active_signals": len(outcome_tracker.active_records),
            "win_rate_pct": round((row[0] or 0) * 100, 1),
            "avg_rr": round(row[1] or 0, 2),
            "avg_mae_pct": round(row[2] or 0, 3),
            "avg_mfe_pct": round(row[3] or 0, 3),
            "edge_discovery_next_at": EDGE_DISCOVERY_MIN_SIGNALS - resolved_since_last_edge_run
        }
    except Exception as e:
        return {"error": str(e)}

@app.get("/edge/discover")
async def manual_edge_discovery():
    """Manually trigger edge discovery"""
    results = await edge_engine.run_analysis(db_conn)
    await send_edge_discovery_report(results)
    return results


# ============ MAIN ============
async def main():
    global db_conn

    try:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        db_conn = await aiosqlite.connect(DB_PATH)
        await init_database(db_conn)

        exchange = ccxt.okx({
            "enableRateLimit": True,
            "options": {
                "defaultType": "spot",
                "fetchOpenInterest": True,
                "fetchFundingRateHistory": True
            },
            "rateLimit": 500,
            "timeout": 30000,
            "verbose": False,
        })

        log.info("🚀 ROMEOTPT v7.0 — SIGNAL HARVESTER + EDGE DISCOVERY")
        log.info("MODE: Accept all signals above floor, log 50+ variables, find real edge")
        log.info(f"Min quality floor: {MIN_QUALITY_SCORE} | Min RR: 1.0")
        log.info(f"Edge discovery triggers at: {EDGE_DISCOVERY_MIN_SIGNALS} resolved signals")
        log.info(f"Scan interval: {SCAN_INTERVAL}s | Top {TOP_N} symbols")

        await v7_scanner_main(exchange)

    except Exception as e:
        log.error(f"Fatal error: {e}")
        import traceback
        log.error(traceback.format_exc())
    finally:
        if db_conn:
            await db_conn.close()
        log.info("Scanner shutdown complete")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--http", action="store_true", help="Run HTTP server")
    args = parser.parse_args()

    if args.http:
        uvicorn.run(app, host="0.0.0.0", port=8000)
    else:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            log.info("Scanner stopped by user")